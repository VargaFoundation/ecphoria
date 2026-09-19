//! Typed business facts: the `kind` vocabulary, the subject grammar each kind must follow, and
//! per-kind JSON Schema validation of `metadata`.
//!
//! A memory store that accepts anything becomes a place where nothing can be found. The fix is
//! not to lock the store down — arbitrary metadata stays arbitrary — but to give the handful of
//! facts a team actually reasons about (decisions, conventions, incidents, ticket summaries, run
//! lessons, flaky tests, hotspots, findings) a **shape** and a **key**:
//!
//! - the shape is a JSON Schema per kind, in `crates/ecphoria-core/schemas/facts/`, validated
//!   here on the write path so a malformed fact is refused at the door rather than discovered
//!   months later by whoever needed it;
//! - the key is the `subject` grammar — `incident:<service>:<date>`, `flaky_test:<path>::<name>`
//!   — which is what makes supersession work. Two writers who follow it land on the same subject
//!   and the newer fact replaces the older; two writers who do not both stay active, and the
//!   store now holds a contradiction nobody will notice.
//!
//! Validation is **off by default** and configured per tenant (`[memory.governance]`): a store
//! that already holds untyped memories must not start refusing its own writers on upgrade. `warn`
//! records what would have been refused (metric + log), `strict` refuses it.

use std::collections::HashMap;
use std::sync::OnceLock;

use serde::Deserialize;
use serde_json::Value;

use super::schema::Validator;

/// How much a tenant's writes are held to the fact schemas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FactValidation {
    /// Accept anything (today's behaviour, and the default).
    #[default]
    Off,
    /// Accept, but count and log what `strict` would have refused. The migration mode: turn it on,
    /// watch `ecphoria_fact_validation_failures_total`, fix the writers, then switch to `strict`.
    Warn,
    /// Refuse a write that does not validate (HTTP 422 at the gateway).
    Strict,
}

impl FactValidation {
    pub fn is_off(self) -> bool {
        self == FactValidation::Off
    }
}

/// The fact vocabulary. Mirrors the `MemoryKind` a Choregos fact carries, so a fact written by the
/// orchestrator and a fact written by hand are the same kind of thing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FactKind {
    Decision,
    Convention,
    Incident,
    TicketSummary,
    RunLesson,
    FlakyTest,
    Hotspot,
    Finding,
    /// The escape hatch: envelope only, no subject grammar.
    Other,
}

impl FactKind {
    pub const ALL: [FactKind; 9] = [
        FactKind::Decision,
        FactKind::Convention,
        FactKind::Incident,
        FactKind::TicketSummary,
        FactKind::RunLesson,
        FactKind::FlakyTest,
        FactKind::Hotspot,
        FactKind::Finding,
        FactKind::Other,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            FactKind::Decision => "decision",
            FactKind::Convention => "convention",
            FactKind::Incident => "incident",
            FactKind::TicketSummary => "ticket_summary",
            FactKind::RunLesson => "run_lesson",
            FactKind::FlakyTest => "flaky_test",
            FactKind::Hotspot => "hotspot",
            FactKind::Finding => "finding",
            FactKind::Other => "other",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        FactKind::ALL.into_iter().find(|k| k.as_str() == s)
    }

    /// The JSON Schema document for this kind (the file's text, as embedded at build time).
    pub fn schema_json(self) -> &'static str {
        match self {
            FactKind::Decision => include_str!("../../schemas/facts/decision.schema.json"),
            FactKind::Convention => include_str!("../../schemas/facts/convention.schema.json"),
            FactKind::Incident => include_str!("../../schemas/facts/incident.schema.json"),
            FactKind::TicketSummary => {
                include_str!("../../schemas/facts/ticket_summary.schema.json")
            }
            FactKind::RunLesson => include_str!("../../schemas/facts/run_lesson.schema.json"),
            FactKind::FlakyTest => include_str!("../../schemas/facts/flaky_test.schema.json"),
            FactKind::Hotspot => include_str!("../../schemas/facts/hotspot.schema.json"),
            FactKind::Finding => include_str!("../../schemas/facts/finding.schema.json"),
            FactKind::Other => include_str!("../../schemas/facts/other.schema.json"),
        }
    }

    /// The regex a normalized `subject` must match, and the human-readable form beside it.
    ///
    /// Patterns are lowercase because [`super::cognition::normalize_subject`] lowercases the
    /// subject before it is stored and before it is looked up — validating the raw input instead
    /// would accept keys that then fail to match each other. `[a-z0-9][a-z0-9._/-]*` is one
    /// segment: a slug, a service name, a tracker key or a path fragment.
    pub fn subject_grammar(self) -> Option<(&'static str, &'static str)> {
        match self {
            FactKind::Decision => Some((
                "decision:<area>:<slug>",
                r"^decision:[a-z0-9][a-z0-9._/-]*:[a-z0-9][a-z0-9._/-]*$",
            )),
            FactKind::Convention => Some((
                "convention:<area>:<slug>",
                r"^convention:[a-z0-9][a-z0-9._/-]*:[a-z0-9][a-z0-9._/-]*$",
            )),
            FactKind::Incident => Some((
                "incident:<service>:<yyyy-mm-dd>",
                r"^incident:[a-z0-9][a-z0-9._/-]*:\d{4}-\d{2}-\d{2}$",
            )),
            FactKind::TicketSummary => Some((
                "ticket:<tracker>:<key>",
                r"^ticket:[a-z0-9][a-z0-9._/-]*:[a-z0-9][a-z0-9._/-]*$",
            )),
            FactKind::RunLesson => Some((
                "run_lesson:<workflow>:<slug>",
                r"^run_lesson:[a-z0-9][a-z0-9._/-]*:[a-z0-9][a-z0-9._/-]*$",
            )),
            // A test path holds `/` and `.` and the name is whatever the framework calls it, so
            // the only structure worth demanding is the `::` that separates them.
            FactKind::FlakyTest => Some(("flaky_test:<path>::<name>", r"^flaky_test:[^:]+::\S.*$")),
            FactKind::Hotspot => Some(("hotspot:<path>", r"^hotspot:\S+$")),
            FactKind::Finding => Some((
                "finding:<tool>:<rule>",
                r"^finding:[a-z0-9][a-z0-9._/-]*:[a-z0-9][a-z0-9._/-]*$",
            )),
            FactKind::Other => None,
        }
    }
}

/// Compiled schemas + subject regexes, built once on first use.
struct Compiled {
    envelope: Validator,
    per_kind: HashMap<&'static str, Validator>,
    subjects: HashMap<&'static str, regex::Regex>,
}

fn compiled() -> &'static Compiled {
    static COMPILED: OnceLock<Compiled> = OnceLock::new();
    COMPILED.get_or_init(|| {
        // `expect` here is deliberate: the schemas are compiled into the binary and covered by
        // `every_embedded_schema_compiles`, so a failure means a broken build, not bad input.
        let envelope: Value =
            serde_json::from_str(include_str!("../../schemas/facts/_envelope.schema.json"))
                .expect("envelope schema is valid JSON");
        let mut per_kind = HashMap::new();
        let mut subjects = HashMap::new();
        for kind in FactKind::ALL {
            let doc: Value =
                serde_json::from_str(kind.schema_json()).expect("fact schema is valid JSON");
            per_kind.insert(
                kind.as_str(),
                Validator::compile(&doc).expect("fact schema compiles"),
            );
            if let Some((_, pattern)) = kind.subject_grammar() {
                subjects.insert(
                    kind.as_str(),
                    regex::Regex::new(pattern).expect("subject grammar compiles"),
                );
            }
        }
        Compiled {
            envelope: Validator::compile(&envelope).expect("envelope schema compiles"),
            per_kind,
            subjects,
        }
    })
}

/// Everything wrong with one fact, as a list a writer can act on in a single round-trip.
///
/// Empty means valid. The caller decides what an error *costs* (nothing in `warn`, a 422 in
/// `strict`) — this function only says what is wrong.
pub fn validate(subject: Option<&str>, metadata: &Value) -> Vec<String> {
    let c = compiled();
    let mut errors = c.envelope.validate(metadata);

    let Some(kind_value) = metadata.get("kind") else {
        // An untyped memory. The envelope still applied (above); nothing more to check.
        return errors;
    };
    let Some(kind_str) = kind_value.as_str() else {
        // The envelope already reported the type error; do not repeat it.
        return errors;
    };
    let Some(kind) = FactKind::parse(kind_str) else {
        // Also reported by the envelope's `enum`; say it in the fact's own vocabulary too, since
        // that is the message a writer can act on.
        errors.push(format!(
            "metadata.kind: unknown fact kind `{kind_str}` — known kinds: {}",
            FactKind::ALL
                .iter()
                .map(|k| k.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        return errors;
    };

    if let Some(validator) = c.per_kind.get(kind.as_str()) {
        errors.extend(
            validator
                .validate(metadata)
                .into_iter()
                .map(|e| format!("metadata.{e}").replace("metadata.<root>", "metadata")),
        );
    }

    if let Some(re) = c.subjects.get(kind.as_str()) {
        let (form, _) = kind
            .subject_grammar()
            .expect("grammar exists if regex does");
        match subject {
            None | Some("") => errors.push(format!(
                "subject: a `{}` fact needs a subject of the form `{form}` — without it nothing \
                 supersedes anything",
                kind.as_str()
            )),
            Some(s) if !re.is_match(s) => errors.push(format!(
                "subject: `{s}` does not match the `{}` grammar `{form}`",
                kind.as_str()
            )),
            Some(_) => {}
        }
    }

    errors
}

/// Is this write attributable?
///
/// Two things count, and nothing else: an explicit `metadata.provenance.source`, or the episodic
/// events the memory was distilled from (`source_event_ids`), which point at records already in
/// the store. An empty `provenance: {}` — what a client sends when it has nothing — does not,
/// otherwise the requirement would be satisfied by the absence of an answer.
pub fn has_provenance(metadata: &Value, source_event_ids: &[uuid::Uuid]) -> bool {
    if !source_event_ids.is_empty() {
        return true;
    }
    metadata
        .get("provenance")
        .and_then(|p| p.get("source"))
        .and_then(Value::as_str)
        .is_some_and(|s| !s.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn every_embedded_schema_compiles_and_uses_supported_keywords_only() {
        // Compiling is the assertion: `Validator::compile` rejects an unsupported keyword, so a
        // schema that grows a `$ref` or a `oneOf` fails here rather than silently validating less.
        let _ = compiled();
        for kind in FactKind::ALL {
            let doc: Value = serde_json::from_str(kind.schema_json()).unwrap();
            Validator::compile(&doc).unwrap_or_else(|e| panic!("{}: {e}", kind.as_str()));
        }
    }

    #[test]
    fn kinds_round_trip_through_their_wire_name() {
        for kind in FactKind::ALL {
            assert_eq!(FactKind::parse(kind.as_str()), Some(kind));
        }
        assert_eq!(FactKind::parse("Decision"), None);
        assert_eq!(FactKind::parse("nonsense"), None);
    }

    #[test]
    fn a_well_formed_incident_passes() {
        let errors = validate(
            Some("incident:checkout-api:2026-09-14"),
            &json!({
                "kind": "incident",
                "service": "checkout-api",
                "occurred_at": "2026-09-14T03:12:00Z",
                "severity": "sev2",
                "paths": ["services/checkout/**"],
                "provenance": {"source": "pagerduty", "ref": "PD-4412"},
            }),
        );
        assert!(errors.is_empty(), "{errors:?}");
    }

    #[test]
    fn an_incident_without_service_or_date_is_refused() {
        let errors = validate(
            Some("incident:checkout-api:2026-09-14"),
            &json!({"kind": "incident"}),
        );
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert!(errors.iter().any(|e| e.contains("`service`")), "{errors:?}");
        assert!(
            errors.iter().any(|e| e.contains("`occurred_at`")),
            "{errors:?}"
        );
    }

    #[test]
    fn the_subject_grammar_is_what_makes_supersession_work() {
        // Right kind, wrong key shape: two writers would produce two active memories.
        let errors = validate(
            Some("the checkout outage"),
            &json!({"kind": "incident", "service": "checkout", "occurred_at": "2026-09-14T03:12:00Z"}),
        );
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(
            errors[0].contains("incident:<service>:<yyyy-mm-dd>"),
            "{}",
            errors[0]
        );
    }

    #[test]
    fn a_missing_subject_on_a_typed_fact_is_an_error() {
        let errors = validate(None, &json!({"kind": "decision", "status": "accepted"}));
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("decision:<area>:<slug>"));
    }

    #[test]
    fn each_kind_accepts_its_documented_subject_form() {
        let cases = [
            ("decision", "decision:billing:use-stripe", json!({})),
            (
                "convention",
                "convention:api:no-breaking-changes",
                json!({}),
            ),
            (
                "incident",
                "incident:payments:2026-01-02",
                json!({"service": "payments", "occurred_at": "2026-01-02T00:00:00Z"}),
            ),
            (
                "ticket_summary",
                "ticket:jira:proj-123",
                json!({"tracker": "jira", "key": "PROJ-123"}),
            ),
            (
                "run_lesson",
                "run_lesson:release-train:flaky-migrations",
                json!({"workflow": "release-train"}),
            ),
            (
                "flaky_test",
                "flaky_test:tests/api/test_orders.py::test_total",
                json!({"test_path": "tests/api/test_orders.py", "test_name": "test_total"}),
            ),
            (
                "hotspot",
                "hotspot:src/billing/invoice.rs",
                json!({"path": "src/billing/invoice.rs"}),
            ),
            (
                "finding",
                "finding:semgrep:python.lang.security.audit",
                json!({"tool": "semgrep", "rule": "python.lang.security.audit"}),
            ),
        ];
        for (kind, subject, extra) in cases {
            let mut metadata = json!({"kind": kind});
            for (k, v) in extra.as_object().unwrap() {
                metadata[k] = v.clone();
            }
            let errors = validate(Some(subject), &metadata);
            assert!(errors.is_empty(), "{kind}: {errors:?}");
        }
    }

    #[test]
    fn other_is_the_escape_hatch_and_imposes_no_grammar() {
        assert!(validate(None, &json!({"kind": "other"})).is_empty());
        assert!(validate(Some("anything at all"), &json!({"kind": "other"})).is_empty());
    }

    #[test]
    fn an_untyped_memory_is_still_held_to_the_envelope() {
        assert!(validate(Some("user.city"), &json!({"note": "free form"})).is_empty());
        let errors = validate(Some("user.city"), &json!({"paths": "src/main.rs"}));
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("expected array"), "{}", errors[0]);
    }

    #[test]
    fn an_unknown_kind_names_the_vocabulary() {
        let errors = validate(Some("x:y:z"), &json!({"kind": "brainwave"}));
        assert!(
            errors
                .iter()
                .any(|e| e.contains("unknown fact kind `brainwave`")),
            "{errors:?}"
        );
        assert!(
            errors.iter().any(|e| e.contains("ticket_summary")),
            "{errors:?}"
        );
    }

    #[test]
    fn an_empty_provenance_object_does_not_count_as_provenance() {
        assert!(!has_provenance(&json!({}), &[]));
        assert!(!has_provenance(&json!({"provenance": {}}), &[]));
        assert!(!has_provenance(
            &json!({"provenance": {"ref": "PR-12"}}),
            &[]
        ));
        assert!(!has_provenance(
            &json!({"provenance": {"source": "   "}}),
            &[]
        ));
        assert!(has_provenance(
            &json!({"provenance": {"source": "gitlab"}}),
            &[]
        ));
    }

    #[test]
    fn source_event_ids_are_provenance_on_their_own() {
        assert!(has_provenance(&json!({}), &[uuid::Uuid::new_v4()]));
    }

    #[test]
    fn validation_modes_parse_from_config_text() {
        #[derive(Deserialize)]
        struct Holder {
            mode: FactValidation,
        }
        let parsed: Holder = toml::from_str(r#"mode = "strict""#).unwrap();
        assert_eq!(parsed.mode, FactValidation::Strict);
        assert!(toml::from_str::<Holder>(r#"mode = "nonsense""#).is_err());
        assert!(FactValidation::default().is_off());
    }

    #[test]
    fn a_ticket_summary_needs_a_known_tracker() {
        let errors = validate(
            Some("ticket:notion:abc"),
            &json!({"kind": "ticket_summary", "tracker": "notion", "key": "ABC"}),
        );
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].starts_with("metadata.tracker:"), "{}", errors[0]);
    }

    #[test]
    fn provenance_shape_is_checked_even_when_it_is_not_required() {
        let errors = validate(
            Some("decision:api:versioning"),
            &json!({"kind": "decision", "provenance": {"ref": "ADR-7"}}),
        );
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(
            errors[0].contains("missing required field `source`"),
            "{}",
            errors[0]
        );
    }

    // ── Fuzzing the write path's validators ──────────────────────────────────────────
    //
    // `validate` runs on every memory write, including ones that arrive from a webhook or an
    // untrusted agent. A panic here is a denial of service on the whole write path, so the
    // property is simply: whatever `metadata` is, this returns.

    use proptest::prelude::*;

    fn arb_json() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::from),
            any::<i64>().prop_map(Value::from),
            any::<f64>()
                .prop_filter("finite", |f| f.is_finite())
                .prop_map(Value::from),
            ".*".prop_map(Value::from),
        ];
        leaf.prop_recursive(4, 32, 8, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..8).prop_map(Value::Array),
                prop::collection::hash_map(".*", inner, 0..8)
                    .prop_map(|m| Value::Object(m.into_iter().collect())),
            ]
        })
    }

    /// Metadata shaped like a real fact — so the generator spends its budget on the paths that
    /// actually branch, instead of on documents that fail the first type check.
    fn arb_fact_metadata() -> impl Strategy<Value = Value> {
        (
            prop_oneof![
                Just(None::<String>),
                prop::sample::select(
                    FactKind::ALL
                        .iter()
                        .map(|k| k.as_str().to_string())
                        .collect::<Vec<_>>()
                )
                .prop_map(Some),
                ".{0,12}".prop_map(Some),
            ],
            arb_json(),
        )
            .prop_map(|(kind, extra)| {
                let mut obj = match extra {
                    Value::Object(map) => map,
                    _ => serde_json::Map::new(),
                };
                if let Some(k) = kind {
                    obj.insert("kind".into(), Value::from(k));
                }
                Value::Object(obj)
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(400))]

        #[test]
        fn validate_never_panics(subject in prop::option::of(".{0,64}"), metadata in arb_json()) {
            let _ = validate(subject.as_deref(), &metadata);
        }

        #[test]
        fn validate_never_panics_on_fact_shaped_metadata(
            subject in prop::option::of("[a-z_]{0,10}:[^\\s]{0,20}:?[^\\s]{0,20}"),
            metadata in arb_fact_metadata(),
        ) {
            let _ = validate(subject.as_deref(), &metadata);
        }

        /// Deterministic: the same input must produce the same verdict. A validator that depends on
        /// map iteration order would refuse a write intermittently, which is the worst way for a
        /// rule to behave.
        #[test]
        fn validate_is_deterministic(
            subject in prop::option::of(".{0,32}"),
            metadata in arb_fact_metadata(),
        ) {
            let first = validate(subject.as_deref(), &metadata);
            let second = validate(subject.as_deref(), &metadata);
            prop_assert_eq!(first, second);
        }

        /// An accepted fact is accepted for a reason the *kind* can explain: whenever validation
        /// passes with a known kind, the subject really does match that kind's grammar.
        #[test]
        fn an_accepted_typed_fact_matches_its_grammar(
            subject in "[a-z_]{1,12}:[a-z0-9._/-]{1,20}:[a-z0-9._/-]{1,20}",
            metadata in arb_fact_metadata(),
        ) {
            if validate(Some(&subject), &metadata).is_empty() {
                if let Some(kind) = metadata
                    .get("kind")
                    .and_then(Value::as_str)
                    .and_then(FactKind::parse)
                {
                    if let Some((_, pattern)) = kind.subject_grammar() {
                        prop_assert!(regex::Regex::new(pattern).unwrap().is_match(&subject));
                    }
                }
            }
        }
    }
}
