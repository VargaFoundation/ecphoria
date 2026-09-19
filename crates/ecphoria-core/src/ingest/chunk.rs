//! Structure-aware Markdown chunking.
//!
//! ## Why documents cannot be one memory each
//!
//! The cognition layer's unit is one memory = one text = one embedding = one BM25 document. That
//! fits a distilled fact. It does not fit a 40 KB runbook: a single vector cannot represent a
//! document that covers ten topics, BM25 length-normalisation buries it against short reference
//! pages, and a hit returns the *whole file* — too large to put in a model's context and too
//! coarse to point a reader at the right paragraph.
//!
//! ## What a chunk is here
//!
//! Sections, not windows. Markdown already carries the author's own segmentation in its heading
//! hierarchy, and that segmentation is *stable across edits* in a way that character offsets are
//! not — which is what makes the bi-temporal story work. Each chunk is addressed by its heading
//! trail (`deployment.md#Kubernetes > Production Values`), so when the document is re-imported:
//!
//! - a section whose text is unchanged is `Confirmed` — no new row;
//! - a section whose text changed **supersedes** its previous version, and the old text stays
//!   queryable through the supersession chain and `as_of`;
//! - a section that disappeared is expired by the document-level sweep;
//! - a section that was added is inserted.
//!
//! So an evolving document keeps its history *per section* rather than per file, and "what did
//! this runbook say about failover in March" is answerable. That falls out of the existing
//! deterministic subject-contradiction path — no new cognition, just a stable subject key.
//!
//! ## Rules
//!
//! - **Never split inside a fenced code block.** A half a code sample is worse than useless: it
//!   retrieves on the same terms but cannot be run or trusted.
//! - **Every chunk carries its heading trail** as a prefix. A section titled "Production Values"
//!   is unfindable by "kubernetes production values" unless its ancestors travel with it.
//! - **Heading-only sections are skipped.** A heading whose body is empty because it exists only
//!   to contain sub-headings carries no text of its own, and its title already travels with every
//!   descendant's trail — emitting it would add an embedding with nothing in it.
//! - **Oversized sections are packed by paragraph**, never mid-sentence, with the heading trail
//!   repeated on each part.
//!
//! Note what is deliberately *absent*: no rule merges a short section into a neighbour. Size-based
//! merging makes a chunk's address depend on its neighbours' lengths, so editing one paragraph can
//! silently re-address unrelated sections — and re-addressing is what the bi-temporal history is
//! keyed on. A small chunk costs one embedding; an unstable address costs the feature.

/// One retrievable piece of a document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// Heading trail, outermost first — e.g. `["Deployment Guide", "Kubernetes", "Production Values"]`.
    /// Empty for content that appears before any heading (a preamble).
    pub heading_path: Vec<String>,
    /// The chunk body, prefixed with its heading trail for retrieval context.
    pub text: String,
    /// 0-based position within the document, for reassembly and neighbour expansion.
    pub ordinal: usize,
}

impl Chunk {
    /// Stable address of this chunk within its document: `path#Heading > Sub-heading`.
    ///
    /// Used as the memory `subject`, which is what makes re-import supersede the previous version
    /// of *this section* rather than duplicating it. Sections split for size get a `~n` suffix so
    /// the parts stay distinct.
    pub fn subject(&self, doc_path: &str, part: Option<usize>) -> String {
        let trail = self.heading_path.join(" > ");
        let base = if trail.trim().is_empty() {
            // No usable heading trail. That is the preamble (ordinal 0) — and also, awkwardly, any
            // section under an *empty* heading (`# ` on a line of its own), whose trail joins to
            // nothing. Both addressed as `doc_path` meant the two collided: on re-import each
            // superseded the other and the document silently kept one of them.
            //
            // The preamble keeps the bare path — it is by far the common case, and changing it
            // would re-address every already-imported document — while a later blank-titled
            // section falls back to its position. Found by `subjects_are_unique_within_a_document`.
            if self.ordinal == 0 {
                doc_path.to_string()
            } else {
                format!("{doc_path}#§{}", self.ordinal)
            }
        } else {
            format!("{doc_path}#{trail}")
        };
        match part {
            Some(n) if n > 0 => format!("{base}~{n}"),
            _ => base,
        }
    }
}

/// Sizing knobs. Defaults target ~1 500 characters — roughly 350–400 tokens, enough for a
/// self-contained section without swamping a context window when several are retrieved.
#[derive(Debug, Clone, Copy)]
pub struct ChunkOptions {
    /// Preferred size in characters when a section has to be split.
    pub target_chars: usize,
    /// Hard ceiling — a section larger than this is packed into several chunks.
    pub max_chars: usize,
}

impl Default for ChunkOptions {
    fn default() -> Self {
        Self {
            target_chars: 1_500,
            max_chars: 3_000,
        }
    }
}

/// A heading line and its level, or `None` for body text. Fence-aware: `#` inside a code block is
/// a comment, not a heading.
fn heading_of(line: &str) -> Option<(usize, String)> {
    let trimmed = line.trim_start();
    if !trimmed.starts_with('#') {
        return None;
    }
    let level = trimmed.chars().take_while(|c| *c == '#').count();
    if !(1..=6).contains(&level) {
        return None;
    }
    let rest = &trimmed[level..];
    // ATX headings require a space after the hashes — `#hashtag` is not a heading.
    if !rest.starts_with(' ') {
        return None;
    }
    Some((level, rest.trim().trim_end_matches('#').trim().to_string()))
}

/// Does this line open or close a fenced code block?
fn is_fence(line: &str) -> bool {
    let t = line.trim_start();
    t.starts_with("```") || t.starts_with("~~~")
}

/// A document section: a heading trail plus the raw lines beneath it.
struct Section {
    heading_path: Vec<String>,
    body: String,
}

/// Split `markdown` into sections at heading boundaries, tracking the heading stack and ignoring
/// headings that appear inside fenced code.
fn split_sections(markdown: &str) -> Vec<Section> {
    let mut sections: Vec<Section> = Vec::new();
    let mut stack: Vec<(usize, String)> = Vec::new();
    let mut current = Section {
        heading_path: Vec::new(),
        body: String::new(),
    };
    let mut in_fence = false;

    for line in markdown.lines() {
        if is_fence(line) {
            in_fence = !in_fence;
            current.body.push_str(line);
            current.body.push('\n');
            continue;
        }
        match (in_fence, heading_of(line)) {
            (false, Some((level, title))) => {
                if !current.body.trim().is_empty() || !current.heading_path.is_empty() {
                    sections.push(current);
                }
                stack.retain(|(l, _)| *l < level);
                stack.push((level, title));
                current = Section {
                    heading_path: stack.iter().map(|(_, t)| t.clone()).collect(),
                    body: String::new(),
                };
            }
            _ => {
                current.body.push_str(line);
                current.body.push('\n');
            }
        }
    }
    if !current.body.trim().is_empty() || !current.heading_path.is_empty() {
        sections.push(current);
    }
    sections
}

/// Split `body` into fence-safe paragraph groups no larger than `max_chars`.
///
/// Paragraphs are blank-line separated. A fenced code block counts as one indivisible paragraph,
/// however long — splitting it would corrupt it, so an oversized fence is emitted whole and the
/// caller gets a chunk above `max_chars` rather than a broken one.
fn pack_paragraphs(body: &str, max_chars: usize) -> Vec<String> {
    // Group lines into paragraphs, keeping fenced blocks intact.
    let mut paragraphs: Vec<String> = Vec::new();
    let mut buf = String::new();
    let mut in_fence = false;
    for line in body.lines() {
        if is_fence(line) {
            in_fence = !in_fence;
            buf.push_str(line);
            buf.push('\n');
            // A closing fence ends the paragraph.
            if !in_fence {
                paragraphs.push(std::mem::take(&mut buf));
            }
            continue;
        }
        if !in_fence && line.trim().is_empty() {
            if !buf.trim().is_empty() {
                paragraphs.push(std::mem::take(&mut buf));
            } else {
                buf.clear();
            }
            continue;
        }
        buf.push_str(line);
        buf.push('\n');
    }
    if !buf.trim().is_empty() {
        paragraphs.push(buf);
    }

    let mut out: Vec<String> = Vec::new();
    let mut current = String::new();
    for p in paragraphs {
        if !current.is_empty() && current.len() + p.len() > max_chars {
            out.push(std::mem::take(&mut current));
        }
        current.push_str(&p);
        current.push('\n');
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}

/// Chunk a Markdown document into retrievable, stably-addressed sections.
///
/// Returns `(chunk, part)` where `part` is the 0-based index of this piece *within* its section —
/// non-zero only when a section had to be split for size. Feed both to [`Chunk::subject`].
pub fn chunk_markdown(markdown: &str, opts: &ChunkOptions) -> Vec<(Chunk, usize)> {
    let sections = split_sections(markdown);
    let mut out: Vec<(Chunk, usize)> = Vec::new();

    for section in sections {
        let trail = section.heading_path.join(" > ");
        let body = section.body.trim();
        // A heading that exists only to hold sub-headings has no text of its own, and its title
        // already travels with every descendant's trail. Emitting it would index an empty chunk.
        if body.is_empty() {
            continue;
        }

        let standalone_len = trail.len() + body.len();
        let parts = if standalone_len <= opts.max_chars {
            vec![body.to_string()]
        } else {
            pack_paragraphs(body, opts.target_chars)
        };
        for (part_idx, part) in parts.into_iter().enumerate() {
            // Prefix the heading trail so the chunk is findable by its ancestors' words, not only
            // its own — "Production Values" alone never matches "kubernetes production values".
            let text = if trail.is_empty() {
                part.trim().to_string()
            } else {
                format!("{trail}\n\n{}", part.trim())
            };
            if text.trim().is_empty() {
                continue;
            }
            let ordinal = out.len();
            out.push((
                Chunk {
                    heading_path: section.heading_path.clone(),
                    text,
                    ordinal,
                },
                part_idx,
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunks(md: &str) -> Vec<Chunk> {
        chunk_markdown(md, &ChunkOptions::default())
            .into_iter()
            .map(|(c, _)| c)
            .collect()
    }

    const DOC: &str = "\
# Deployment Guide

Intro paragraph that is long enough to stand on its own as a chunk rather than being
merged upward into nothing, because the merge threshold is two hundred characters and
this preamble comfortably clears it with room to spare.

## Docker

Run the container with a volume mount so data survives a restart. This section also needs
to be long enough to survive the minimum-size merge, so here is some more prose about
mounting volumes and choosing ports for the deployment.

## Kubernetes

Use the Helm chart. The chart deploys a StatefulSet and a headless service, and this
paragraph exists to push the section past the minimum chunk size threshold.

### Production Values

Set replicas to three and enable the pod disruption budget, because a two-node Raft
cluster cannot tolerate any failure at all and that surprises people regularly.
";

    #[test]
    fn splits_on_headings_and_tracks_the_trail() {
        let cs = chunks(DOC);
        let trails: Vec<String> = cs.iter().map(|c| c.heading_path.join(" > ")).collect();
        assert_eq!(
            trails,
            vec![
                "Deployment Guide",
                "Deployment Guide > Docker",
                "Deployment Guide > Kubernetes",
                "Deployment Guide > Kubernetes > Production Values",
            ]
        );
        assert_eq!(cs[3].ordinal, 3);
    }

    #[test]
    fn every_chunk_carries_its_ancestors_words() {
        // The point of the trail prefix: a deep section is findable by its parents' terms.
        let cs = chunks(DOC);
        let deep = cs.last().unwrap();
        assert!(deep.text.contains("Kubernetes"), "{}", deep.text);
        assert!(deep.text.contains("Production Values"));
        assert!(deep.text.contains("disruption budget"));
    }

    #[test]
    fn subject_is_a_stable_address() {
        let cs = chunks(DOC);
        assert_eq!(
            cs[3].subject("docs/deployment.md", None),
            "docs/deployment.md#Deployment Guide > Kubernetes > Production Values"
        );
        // Size-split parts stay distinct.
        assert_eq!(
            cs[3].subject("docs/deployment.md", Some(1)),
            "docs/deployment.md#Deployment Guide > Kubernetes > Production Values~1"
        );
        // Preamble with no heading addresses the document itself.
        assert_eq!(
            Chunk {
                heading_path: vec![],
                text: "x".into(),
                ordinal: 0
            }
            .subject("README.md", None),
            "README.md"
        );
    }

    #[test]
    fn editing_one_section_leaves_the_others_addressed_identically() {
        // This is what makes per-section bi-temporal history work: the subjects of untouched
        // sections must not move when a sibling changes.
        let before = chunks(DOC);
        let after = chunks(&DOC.replace("Set replicas to three", "Set replicas to five"));
        let subj =
            |cs: &[Chunk]| -> Vec<String> { cs.iter().map(|c| c.subject("d.md", None)).collect() };
        assert_eq!(subj(&before), subj(&after), "addresses must be stable");
        assert_eq!(before[0].text, after[0].text, "untouched section unchanged");
        assert_ne!(before[3].text, after[3].text, "edited section changed");
    }

    #[test]
    fn never_splits_inside_a_code_fence() {
        let mut md = String::from("# Runbook\n\nPreamble.\n\n```bash\n");
        // A fence far larger than max_chars — it must still come back whole.
        for i in 0..400 {
            md.push_str(&format!("echo 'step {i} of the recovery procedure'\n"));
        }
        md.push_str("```\n\nTrailing note.\n");

        let cs = chunks(&md);
        let fenced: Vec<&Chunk> = cs.iter().filter(|c| c.text.contains("```")).collect();
        assert!(!fenced.is_empty());
        for c in &cs {
            let opens = c.text.matches("```").count();
            assert_eq!(opens % 2, 0, "unbalanced fence in chunk:\n{}", c.text);
        }
        // Every step survived somewhere.
        let all: String = cs.iter().map(|c| c.text.as_str()).collect();
        assert!(all.contains("step 0 of"));
        assert!(all.contains("step 399 of"));
    }

    #[test]
    fn headings_inside_code_are_not_headings() {
        let md = "# Real\n\nSome introductory prose that is long enough to avoid being merged \
                  upward into a neighbouring chunk by the minimum-size rule applied here.\n\n\
                  ```python\n# not a heading\nx = 1\n```\n";
        let cs = chunks(md);
        assert_eq!(cs.len(), 1, "code comment started a new section: {cs:#?}");
        assert_eq!(cs[0].heading_path, vec!["Real".to_string()]);
    }

    #[test]
    fn oversized_sections_split_at_paragraph_boundaries() {
        let para = "This paragraph is deliberately verbose so that several of them together \
                    exceed the maximum chunk size and force the packer to emit more than one \
                    chunk for a single heading.\n\n";
        let md = format!("# Big\n\n{}", para.repeat(30));
        let parts = chunk_markdown(&md, &ChunkOptions::default());
        assert!(parts.len() > 1, "expected a split, got {}", parts.len());
        // Parts are numbered so their subjects stay distinct.
        assert_eq!(parts[0].1, 0);
        assert_eq!(parts[1].1, 1);
        // No chunk ends mid-sentence: each ends at a paragraph boundary.
        for (c, _) in &parts {
            assert!(
                c.text.trim_end().ends_with("chunk for a single heading."),
                "split mid-paragraph:\n{}",
                c.text
            );
        }
    }

    #[test]
    fn heading_only_sections_are_skipped_but_survive_in_descendant_trails() {
        // "Container" has no body of its own — it only groups sub-sections. It must not become an
        // empty chunk, and its title must still be searchable via its children.
        let md = "# Guide\n\nIntro body.\n\n## Container\n\n### Docker\n\nRun it with a volume.\n";
        let cs = chunks(md);
        let trails: Vec<String> = cs.iter().map(|c| c.heading_path.join(" > ")).collect();
        assert_eq!(trails, vec!["Guide", "Guide > Container > Docker"]);
        assert!(cs[1].text.contains("Container"), "{}", cs[1].text);
    }

    #[test]
    fn short_sections_keep_their_own_address() {
        // Deliberately no size-based merging: a one-line section is still independently
        // addressable, so editing it supersedes only itself.
        let md = "# Main\n\nA body paragraph.\n\n## Note\n\nShort.\n";
        let cs = chunks(md);
        assert_eq!(cs.len(), 2, "{cs:#?}");
        assert_eq!(cs[1].subject("d.md", None), "d.md#Main > Note");
    }

    #[test]
    fn preamble_before_any_heading_is_kept() {
        let md = "Some front matter prose that appears before any heading at all, long enough \
                  to stand alone as its own chunk under the minimum size rule.\n\n# Later\n\n\
                  Body text under the first real heading, also comfortably long enough to be \
                  its own separate chunk in the output.\n";
        let cs = chunks(md);
        assert_eq!(cs.len(), 2);
        assert!(cs[0].heading_path.is_empty());
        assert!(cs[0].text.contains("front matter"));
    }

    #[test]
    fn empty_and_whitespace_documents_produce_nothing() {
        assert!(chunks("").is_empty());
        assert!(chunks("   \n\n  \n").is_empty());
    }

    #[test]
    fn deeper_heading_pops_back_to_the_right_ancestor() {
        let md = "# A\n\nBody of A, long enough to be its own chunk without merging upward into \
                  anything else at all in this particular document.\n\n\
                  ## B\n\nBody of B, also long enough to survive the minimum-size merge rule \
                  that would otherwise fold it into its parent section.\n\n\
                  ### C\n\nBody of C, likewise long enough to stand alone as a chunk in the \
                  output of the markdown chunker under test.\n\n\
                  ## D\n\nBody of D, which must be a sibling of B and not a child of C, and is \
                  long enough to be its own chunk in the result.\n";
        let cs = chunks(md);
        let trails: Vec<String> = cs.iter().map(|c| c.heading_path.join(" > ")).collect();
        assert_eq!(trails, vec!["A", "A > B", "A > B > C", "A > D"]);
    }

    /// A document with a blank heading used to address that section as the document itself,
    /// colliding with the preamble — so re-importing kept one of the two and dropped the other,
    /// silently and for good. Found by `subjects_are_unique_within_a_document`.
    #[test]
    fn a_blank_heading_does_not_collide_with_the_preamble() {
        let chunks = chunk_markdown(
            "intro text\n\n# \n\nbody under a nameless heading",
            &ChunkOptions::default(),
        );
        let subjects: Vec<String> = chunks
            .iter()
            .map(|(c, part)| c.subject("docs/x.md", Some(*part)))
            .collect();
        let unique: std::collections::HashSet<&String> = subjects.iter().collect();
        assert_eq!(unique.len(), subjects.len(), "{subjects:?}");
        // The preamble keeps the bare path, so already-imported documents do not move.
        assert_eq!(subjects[0], "docs/x.md");
    }

    // ── Fuzzing the document parser ──────────────────────────────────────────────────
    //
    // `document_ingest` feeds arbitrary Markdown through here — a README from a repository an
    // importer was pointed at, a page from a wiki. A panic is a denial of service on the ingest
    // path, and a chunker that loses text silently loses a document.

    use proptest::prelude::*;

    /// Markdown-ish text: headings, fences and prose in the proportions that actually exercise the
    /// state machine, rather than uniformly random bytes that are all prose.
    fn arb_markdown() -> impl Strategy<Value = String> {
        let line = prop_oneof![
            "#{1,7} .{0,40}",    // heading (including 7 hashes, which is not one)
            "```.{0,10}",        // fence open/close
            ".{0,60}",           // prose
            Just(String::new()), // blank
            "    .{0,40}",       // indented code
            "[-*] .{0,40}",      // list item
        ];
        prop::collection::vec(line, 0..40).prop_map(|lines| lines.join("\n"))
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(300))]

        #[test]
        fn chunking_never_panics(text in arb_markdown()) {
            let _ = chunk_markdown(&text, &ChunkOptions::default());
        }

        /// Every chunk is addressable: a section with no stable subject cannot be superseded on
        /// re-import, so the document's history would fork silently on the next sync.
        #[test]
        fn every_chunk_has_a_subject(text in arb_markdown()) {
            for (chunk, part) in chunk_markdown(&text, &ChunkOptions::default()) {
                let subject = chunk.subject("docs/x.md", Some(part));
                prop_assert!(!subject.trim().is_empty());
            }
        }

        /// Deterministic: the same document must chunk identically, or a re-import would supersede
        /// sections that did not change.
        #[test]
        fn chunking_is_deterministic(text in arb_markdown()) {
            let a = chunk_markdown(&text, &ChunkOptions::default());
            let b = chunk_markdown(&text, &ChunkOptions::default());
            prop_assert_eq!(a.len(), b.len());
            for ((ca, pa), (cb, pb)) in a.iter().zip(b.iter()) {
                prop_assert_eq!(&ca.text, &cb.text);
                prop_assert_eq!(&ca.heading_path, &cb.heading_path);
                prop_assert_eq!(pa, pb);
            }
        }

        /// Subjects are unique within one document. Two sections sharing a subject would
        /// supersede each other on every import — the document would keep exactly one of them.
        #[test]
        fn subjects_are_unique_within_a_document(text in arb_markdown()) {
            let chunks = chunk_markdown(&text, &ChunkOptions::default());
            let mut seen = std::collections::HashSet::new();
            for (chunk, part) in &chunks {
                let subject = crate::memory::cognition::normalize_subject(
                    &chunk.subject("docs/x.md", Some(*part)),
                );
                prop_assert!(seen.insert(subject.clone()), "duplicate subject: {}", subject);
            }
        }
    }
}
