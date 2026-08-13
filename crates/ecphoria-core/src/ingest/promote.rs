//! Promoting webhook events into memories.
//!
//! Webhooks land in the **episodic** store: a time-ordered log, queryable in SQL, invisible to
//! `search_memory`. That is the right home for the firehose — every push, every label change,
//! every alert flap — but it means the two things a team actually wants to recall later are
//! unreachable by the memory API: *which tickets were handled*, and *what incidents happened*.
//!
//! Promotion is the bridge. A small, opt-in set of event types — the ones that mark a durable
//! outcome rather than a transient state change — is additionally written as a memory, so a
//! question like "have we seen this error before" reaches them.
//!
//! Two design choices worth stating:
//!
//! - **Outcomes, not activity.** The defaults promote closes, merges and resolutions. Promoting
//!   every event would bury the documentation corpus under CI noise, and the noise is already in
//!   episodic if anyone needs it.
//! - **Deterministic subjects.** A promoted memory is keyed `github/acme/api#pr-42`, so a webhook
//!   redelivery (which providers do freely) resolves as `Confirmed` rather than a duplicate, and
//!   an issue that is closed, reopened and closed again supersedes itself into a real history
//!   instead of stacking three copies.

use crate::memory::episodic::Event;

/// A webhook event judged worth remembering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Promotion {
    /// Stable key for this ticket/incident — redelivery confirms, a later state supersedes.
    pub subject: String,
    /// Human-readable summary; this is what gets embedded and BM25-indexed.
    pub content: String,
}

/// Vendor prefix of a normalized event source (`github/acme/api` → `github`).
fn vendor(source: &str) -> &str {
    source.split('/').next().unwrap_or(source)
}

/// Does `event` match any configured rule?
///
/// A rule is `"<vendor>:<event_type>"`, where either side may be `*`, and an event type ending in
/// `.*` matches by prefix (`github:pull_request.*`). Kept to prefix globbing rather than a regex:
/// these live in a config file, and a mistyped regex that silently matches nothing is worse than
/// a syntax with nothing to mistype.
pub fn matches(event: &Event, rules: &[String]) -> bool {
    let v = vendor(&event.source);
    rules.iter().any(|rule| {
        let Some((want_vendor, want_type)) = rule.split_once(':') else {
            return false;
        };
        let vendor_ok = want_vendor == "*" || want_vendor.eq_ignore_ascii_case(v);
        let type_ok = want_type == "*"
            || want_type == event.event_type
            || want_type
                .strip_suffix('*')
                .is_some_and(|p| event.event_type.starts_with(p));
        vendor_ok && type_ok
    })
}

/// Sensible defaults: the moments that represent a durable outcome.
pub fn default_rules() -> Vec<String> {
    vec![
        "github:pull_request.closed".into(),
        "github:issue.closed".into(),
        "sentry:issue.resolved".into(),
        "pagerduty:incident.resolved".into(),
        "pagerduty:incident.resolve".into(),
    ]
}

/// Build the memory a promotable event becomes, or `None` if nothing useful can be extracted.
///
/// Extraction is per-vendor because the useful identity lives in a different place for each: a
/// GitHub PR has a number, a Sentry issue has only a title, a PagerDuty incident has a service.
/// Where no stable identifier exists the title is used — imperfect, but it still collapses
/// redeliveries of the same alert, which is the property that matters.
pub fn promote(event: &Event) -> Option<Promotion> {
    let p = &event.payload;
    let str_at = |ptr: &str| p.pointer(ptr).and_then(|v| v.as_str());
    let repo = str_at("/repository").unwrap_or("unknown");

    match vendor(&event.source) {
        "github" if event.event_type.starts_with("pull_request.") => {
            let number = p.pointer("/raw/pull_request/number")?.as_u64()?;
            let title = str_at("/raw/pull_request/title").unwrap_or("(no title)");
            let merged = p
                .pointer("/raw/pull_request/merged")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let who = str_at("/sender").unwrap_or("someone");
            let outcome = if merged {
                "merged"
            } else {
                "closed without merging"
            };
            Some(Promotion {
                subject: format!("{repo}#pr-{number}"),
                content: format!(
                    "Pull request #{number} in {repo} — \"{title}\" — {outcome} by {who}."
                ),
            })
        }
        "github" if event.event_type.starts_with("issue.") => {
            let number = p.pointer("/raw/issue/number")?.as_u64()?;
            let title = str_at("/raw/issue/title").unwrap_or("(no title)");
            let who = str_at("/sender").unwrap_or("someone");
            let body = str_at("/raw/issue/body").unwrap_or("");
            let mut content = format!("Issue #{number} in {repo} — \"{title}\" — closed by {who}.");
            // The body is where the actual resolution usually is; it is what makes the memory
            // worth having rather than a bare status line.
            if !body.trim().is_empty() {
                content.push_str("\n\n");
                content.push_str(body.trim());
            }
            Some(Promotion {
                subject: format!("{repo}#issue-{number}"),
                content,
            })
        }
        "sentry" => {
            let title = str_at("/title").filter(|t| !t.is_empty())?;
            let project = str_at("/project").unwrap_or("unknown");
            let level = str_at("/level").unwrap_or("error");
            let action = str_at("/action").unwrap_or("resolved");
            Some(Promotion {
                subject: format!("sentry/{project}#{}", slug(title)),
                content: format!("Sentry {level} in {project} — \"{title}\" — {action}."),
            })
        }
        "pagerduty" => {
            let title = str_at("/title").filter(|t| !t.is_empty())?;
            let service = str_at("/service").unwrap_or("unknown");
            Some(Promotion {
                subject: format!("pagerduty/{service}#{}", slug(title)),
                content: format!(
                    "PagerDuty incident on {service} — \"{title}\" — {}.",
                    event.event_type
                ),
            })
        }
        _ => None,
    }
}

/// Lowercase, non-alphanumerics collapsed to `-`, bounded length — a stable key from free text.
fn slug(s: &str) -> String {
    let mut out = String::new();
    let mut last_dash = true;
    for c in s.chars().take(120) {
        if c.is_alphanumeric() {
            out.extend(c.to_lowercase());
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    out.trim_matches('-').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::webhook::normalize_webhook as normalize;

    fn ev(source: &str, payload: serde_json::Value) -> Vec<Event> {
        normalize(source, &payload).expect("normalize")
    }

    #[test]
    fn merged_pull_request_becomes_a_memory() {
        let events = ev(
            "github",
            serde_json::json!({
                "action": "closed",
                "repository": {"full_name": "acme/api"},
                "sender": {"login": "alice"},
                "pull_request": {"number": 42, "title": "Fix the shard router", "merged": true}
            }),
        );
        let p = promote(&events[0]).expect("should promote");
        assert_eq!(p.subject, "acme/api#pr-42");
        assert!(p.content.contains("#42"), "{}", p.content);
        assert!(p.content.contains("Fix the shard router"));
        assert!(p.content.contains("merged by alice"));
    }

    #[test]
    fn closed_issue_carries_its_body() {
        // The body is where the resolution is written — a bare status line would not be worth
        // remembering.
        let events = ev(
            "github",
            serde_json::json!({
                "action": "closed",
                "repository": {"full_name": "acme/api"},
                "sender": {"login": "bob"},
                "issue": {
                    "number": 7,
                    "title": "Quorum loss after failover",
                    "body": "Root cause: the standby had a stale term. Fixed by bumping on promote."
                }
            }),
        );
        let p = promote(&events[0]).expect("should promote");
        assert_eq!(p.subject, "acme/api#issue-7");
        assert!(p.content.contains("Root cause"), "{}", p.content);
    }

    #[test]
    fn redelivery_and_state_changes_share_one_subject() {
        // Providers redeliver freely, and an issue can close, reopen and close again. All of it
        // must collapse onto one key so cognition confirms/supersedes instead of duplicating.
        let mk = |action: &str| {
            ev(
                "github",
                serde_json::json!({
                    "action": action,
                    "repository": {"full_name": "acme/api"},
                    "sender": {"login": "alice"},
                    "issue": {"number": 7, "title": "Flaky test", "body": "x"}
                }),
            )
        };
        let a = promote(&mk("closed")[0]).unwrap();
        let b = promote(&mk("closed")[0]).unwrap();
        let c = promote(&mk("reopened")[0]).unwrap();
        assert_eq!(a.subject, b.subject);
        assert_eq!(a.subject, c.subject);
        assert_eq!(a.content, b.content, "redelivery must be byte-identical");
    }

    #[test]
    fn incidents_and_alerts_promote() {
        let s = ev(
            "sentry",
            serde_json::json!({
                "action": "resolved",
                "data": {"issue": {"project": {"slug": "api"}, "title": "NullPointer in /checkout", "level": "error"}}
            }),
        );
        let p = promote(&s[0]).expect("sentry should promote");
        assert_eq!(p.subject, "sentry/api#nullpointer-in-checkout");
        assert!(p.content.contains("NullPointer in /checkout"));

        let pd = ev(
            "pagerduty",
            serde_json::json!({
                "event": {
                    "event_type": "incident.resolved",
                    "data": {"service": {"name": "checkout"}, "title": "Elevated 5xx"}
                }
            }),
        );
        let p = promote(&pd[0]).expect("pagerduty should promote");
        assert_eq!(p.subject, "pagerduty/checkout#elevated-5xx");
    }

    #[test]
    fn unpromotable_events_yield_nothing() {
        // A push has no durable outcome to remember, and an unknown vendor has no known shape.
        let push = ev(
            "github",
            serde_json::json!({"repository": {"full_name": "acme/api"}, "commits": [{"id": "abc"}]}),
        );
        assert!(promote(&push[0]).is_none());
        let other = ev("slack", serde_json::json!({"event": {"text": "hi"}}));
        assert!(promote(&other[0]).is_none());
    }

    #[test]
    fn rules_match_by_vendor_and_event_type() {
        let pr = ev(
            "github",
            serde_json::json!({
                "action": "closed",
                "repository": {"full_name": "acme/api"},
                "pull_request": {"number": 1, "title": "t"}
            }),
        );
        let e = &pr[0];
        // Source is `github/acme/api` — matching must key on the vendor, not the whole string.
        assert!(matches(e, &["github:pull_request.closed".into()]));
        assert!(matches(e, &["github:pull_request.*".into()]));
        assert!(matches(e, &["github:*".into()]));
        assert!(matches(e, &["*:pull_request.closed".into()]));
        assert!(!matches(e, &["github:issue.closed".into()]));
        assert!(!matches(e, &["sentry:*".into()]));
        assert!(!matches(e, &["malformed-no-colon".into()]));
        assert!(!matches(e, &[]));
        // The shipped defaults must actually fire on a closed PR.
        assert!(matches(e, &default_rules()));
    }
}
