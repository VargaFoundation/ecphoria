//! A small JSON Schema validator — the 2020-12 subset the fact schemas actually use.
//!
//! Why not a schema crate: the only documents validated here are the ones in
//! `crates/ecphoria-core/schemas/`, which this repository writes and reviews. A full validator
//! would bring a resolver, a remote-reference fetcher and a regex engine we do not otherwise need,
//! for keywords no schema of ours uses.
//!
//! The usual failure mode of a hand-rolled validator — a keyword nobody implemented silently
//! passing every document — is closed here: [`Validator::compile`] **rejects** a schema containing
//! a keyword it does not support, and `schemas_use_only_supported_keywords` compiles every
//! embedded schema. Adding `oneOf` to a fact schema therefore breaks the build rather than
//! quietly weakening validation.

use std::collections::BTreeSet;

use serde_json::Value;

/// Keywords this validator understands. Anything else in a schema is a compile error.
const SUPPORTED: &[&str] = &[
    // Annotations — carried for humans, ignored when validating.
    "$schema",
    "$id",
    "title",
    "description",
    "examples",
    "default",
    // Assertions.
    "type",
    "properties",
    "required",
    "additionalProperties",
    "enum",
    "const",
    "pattern",
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
    "minItems",
    "maxItems",
    "uniqueItems",
    "items",
    "format",
];

/// Formats given meaning here. An unknown `format` is a compile error rather than an annotation,
/// so a typo (`date_time`) cannot pass for validation.
const SUPPORTED_FORMATS: &[&str] = &["date-time", "date", "uri", "uuid"];

/// A compiled schema: regexes are built once, at compile time, not per document.
#[derive(Debug, Clone)]
pub struct Validator {
    node: Node,
}

#[derive(Debug, Clone)]
struct Node {
    types: Vec<String>,
    properties: Vec<(String, Node)>,
    required: Vec<String>,
    additional_properties: bool,
    enumeration: Option<Vec<Value>>,
    constant: Option<Value>,
    pattern: Option<regex::Regex>,
    min_length: Option<usize>,
    max_length: Option<usize>,
    minimum: Option<f64>,
    maximum: Option<f64>,
    min_items: Option<usize>,
    max_items: Option<usize>,
    unique_items: bool,
    items: Option<Box<Node>>,
    format: Option<String>,
}

impl Validator {
    /// Compile a schema document.
    ///
    /// Fails on an unsupported keyword, an unknown `format`, or a `pattern` that is not a valid
    /// regex — all three being authoring mistakes that must not reach runtime.
    pub fn compile(schema: &Value) -> Result<Self, String> {
        Ok(Self {
            node: compile_node(schema, "#")?,
        })
    }

    /// Validate `value`, returning **every** violation rather than the first.
    ///
    /// A caller writing a fact wants the whole list in one response: fixing one field only to be
    /// told about the next is the slowest possible way to learn a schema.
    pub fn validate(&self, value: &Value) -> Vec<String> {
        let mut errors = Vec::new();
        check(&self.node, value, "", &mut errors);
        errors
    }
}

fn compile_node(schema: &Value, path: &str) -> Result<Node, String> {
    let obj = schema
        .as_object()
        .ok_or_else(|| format!("{path}: a schema must be an object"))?;

    for key in obj.keys() {
        if !SUPPORTED.contains(&key.as_str()) {
            return Err(format!(
                "{path}: unsupported schema keyword `{key}` — see memory::schema::SUPPORTED"
            ));
        }
    }

    let types = match obj.get("type") {
        None => Vec::new(),
        Some(Value::String(t)) => vec![t.clone()],
        Some(Value::Array(ts)) => ts
            .iter()
            .map(|t| {
                t.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| format!("{path}: `type` entries must be strings"))
            })
            .collect::<Result<_, _>>()?,
        Some(_) => return Err(format!("{path}: `type` must be a string or an array")),
    };

    let mut properties = Vec::new();
    if let Some(props) = obj.get("properties") {
        let props = props
            .as_object()
            .ok_or_else(|| format!("{path}: `properties` must be an object"))?;
        for (name, sub) in props {
            properties.push((name.clone(), compile_node(sub, &format!("{path}/{name}"))?));
        }
    }

    let required = match obj.get("required") {
        None => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| format!("{path}: `required` entries must be strings"))
            })
            .collect::<Result<_, _>>()?,
        Some(_) => return Err(format!("{path}: `required` must be an array")),
    };

    let pattern = match obj.get("pattern") {
        None => None,
        Some(Value::String(p)) => Some(
            regex::Regex::new(p).map_err(|e| format!("{path}: invalid `pattern` regex: {e}"))?,
        ),
        Some(_) => return Err(format!("{path}: `pattern` must be a string")),
    };

    let format = match obj.get("format") {
        None => None,
        Some(Value::String(f)) if SUPPORTED_FORMATS.contains(&f.as_str()) => Some(f.clone()),
        Some(Value::String(f)) => {
            return Err(format!(
                "{path}: unsupported `format` `{f}` — supported: {}",
                SUPPORTED_FORMATS.join(", ")
            ))
        }
        Some(_) => return Err(format!("{path}: `format` must be a string")),
    };

    let items = match obj.get("items") {
        None => None,
        Some(sub) => Some(Box::new(compile_node(sub, &format!("{path}/items"))?)),
    };

    Ok(Node {
        types,
        properties,
        required,
        additional_properties: obj
            .get("additionalProperties")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        enumeration: obj
            .get("enum")
            .and_then(Value::as_array)
            .map(|a| a.to_vec()),
        constant: obj.get("const").cloned(),
        pattern,
        min_length: obj
            .get("minLength")
            .and_then(Value::as_u64)
            .map(|n| n as usize),
        max_length: obj
            .get("maxLength")
            .and_then(Value::as_u64)
            .map(|n| n as usize),
        minimum: obj.get("minimum").and_then(Value::as_f64),
        maximum: obj.get("maximum").and_then(Value::as_f64),
        min_items: obj
            .get("minItems")
            .and_then(Value::as_u64)
            .map(|n| n as usize),
        max_items: obj
            .get("maxItems")
            .and_then(Value::as_u64)
            .map(|n| n as usize),
        unique_items: obj
            .get("uniqueItems")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        items,
        format,
    })
}

/// Human-readable location of a violation: `""` is the document root.
fn at(path: &str) -> String {
    if path.is_empty() {
        "<root>".into()
    } else {
        path.trim_start_matches('.').to_string()
    }
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "integer"
            } else {
                "number"
            }
        }
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn type_matches(expected: &str, value: &Value) -> bool {
    match expected {
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        other => other == type_name(value),
    }
}

fn check(node: &Node, value: &Value, path: &str, errors: &mut Vec<String>) {
    if !node.types.is_empty() && !node.types.iter().any(|t| type_matches(t, value)) {
        errors.push(format!(
            "{}: expected {}, got {}",
            at(path),
            node.types.join(" or "),
            type_name(value)
        ));
        // Every remaining assertion assumes the type; reporting them too would be noise.
        return;
    }

    if let Some(allowed) = &node.enumeration {
        if !allowed.contains(value) {
            let rendered: Vec<String> = allowed.iter().map(render).collect();
            errors.push(format!(
                "{}: must be one of {}",
                at(path),
                rendered.join(", ")
            ));
        }
    }
    if let Some(expected) = &node.constant {
        if value != expected {
            errors.push(format!("{}: must be {}", at(path), render(expected)));
        }
    }

    match value {
        Value::String(s) => check_string(node, s, path, errors),
        Value::Number(_) => {
            let n = value.as_f64().unwrap_or_default();
            if let Some(min) = node.minimum {
                if n < min {
                    errors.push(format!("{}: must be >= {min}", at(path)));
                }
            }
            if let Some(max) = node.maximum {
                if n > max {
                    errors.push(format!("{}: must be <= {max}", at(path)));
                }
            }
        }
        Value::Array(items) => check_array(node, items, path, errors),
        Value::Object(map) => {
            for name in &node.required {
                if !map.contains_key(name) {
                    errors.push(format!("{}: missing required field `{name}`", at(path)));
                }
            }
            for (name, sub) in &node.properties {
                if let Some(child) = map.get(name) {
                    check(sub, child, &format!("{path}.{name}"), errors);
                }
            }
            if !node.additional_properties {
                let known: BTreeSet<&str> =
                    node.properties.iter().map(|(n, _)| n.as_str()).collect();
                for name in map.keys() {
                    if !known.contains(name.as_str()) {
                        errors.push(format!("{}: unknown field `{name}`", at(path)));
                    }
                }
            }
        }
        _ => {}
    }
}

fn check_string(node: &Node, s: &str, path: &str, errors: &mut Vec<String>) {
    // Length is counted in characters, not bytes: a schema author means "40 characters",
    // and a UTF-8 byte count would reject an accented subject that is well within the limit.
    let len = s.chars().count();
    if let Some(min) = node.min_length {
        if len < min {
            errors.push(format!("{}: must be at least {min} characters", at(path)));
        }
    }
    if let Some(max) = node.max_length {
        if len > max {
            errors.push(format!("{}: must be at most {max} characters", at(path)));
        }
    }
    if let Some(re) = &node.pattern {
        if !re.is_match(s) {
            errors.push(format!("{}: must match /{}/", at(path), re.as_str()));
        }
    }
    if let Some(format) = &node.format {
        if !format_ok(format, s) {
            errors.push(format!("{}: not a valid {format}", at(path)));
        }
    }
}

fn check_array(node: &Node, items: &[Value], path: &str, errors: &mut Vec<String>) {
    if let Some(min) = node.min_items {
        if items.len() < min {
            errors.push(format!("{}: must have at least {min} items", at(path)));
        }
    }
    if let Some(max) = node.max_items {
        if items.len() > max {
            errors.push(format!("{}: must have at most {max} items", at(path)));
        }
    }
    if node.unique_items {
        for (i, item) in items.iter().enumerate() {
            if items[..i].contains(item) {
                errors.push(format!("{}: duplicate item {}", at(path), render(item)));
                break;
            }
        }
    }
    if let Some(sub) = &node.items {
        for (i, item) in items.iter().enumerate() {
            check(sub, item, &format!("{path}[{i}]"), errors);
        }
    }
}

fn format_ok(format: &str, s: &str) -> bool {
    match format {
        "date-time" => chrono::DateTime::parse_from_rfc3339(s).is_ok(),
        "date" => chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok(),
        "uuid" => uuid::Uuid::parse_str(s).is_ok(),
        // Deliberately shallow: enough to catch a bare word or a path, not a URL parser.
        "uri" => s.contains("://") && !s.contains(char::is_whitespace),
        _ => true,
    }
}

fn render(value: &Value) -> String {
    match value {
        Value::String(s) => format!("`{s}`"),
        other => format!("`{other}`"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn compile(schema: Value) -> Validator {
        Validator::compile(&schema).expect("schema compiles")
    }

    #[test]
    fn accepts_a_document_that_satisfies_every_assertion() {
        let v = compile(json!({
            "type": "object",
            "required": ["area"],
            "properties": {
                "area": {"type": "string", "minLength": 1},
                "tags": {"type": "array", "items": {"type": "string"}, "uniqueItems": true},
            },
        }));
        assert!(v
            .validate(&json!({"area": "billing", "tags": ["a", "b"]}))
            .is_empty());
    }

    #[test]
    fn reports_every_violation_not_just_the_first() {
        let v = compile(json!({
            "type": "object",
            "required": ["area", "owner"],
            "properties": {"area": {"type": "string"}},
        }));
        let errors = v.validate(&json!({"area": 12}));
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert!(errors
            .iter()
            .any(|e| e.contains("missing required field `owner`")));
        assert!(errors
            .iter()
            .any(|e| e.contains("expected string, got integer")));
    }

    #[test]
    fn an_unsupported_keyword_is_a_compile_error_not_a_silent_pass() {
        let err = Validator::compile(&json!({"oneOf": [{"type": "string"}]})).unwrap_err();
        assert!(err.contains("unsupported schema keyword `oneOf`"), "{err}");
    }

    #[test]
    fn an_unknown_format_is_a_compile_error() {
        let err =
            Validator::compile(&json!({"type": "string", "format": "date_time"})).unwrap_err();
        assert!(err.contains("unsupported `format`"), "{err}");
    }

    #[test]
    fn an_invalid_pattern_is_a_compile_error() {
        let err = Validator::compile(&json!({"type": "string", "pattern": "("})).unwrap_err();
        assert!(err.contains("invalid `pattern` regex"), "{err}");
    }

    #[test]
    fn pattern_and_length_are_enforced_on_strings() {
        let v = compile(json!({"type": "string", "pattern": "^inc-[0-9]+$", "maxLength": 8}));
        assert!(v.validate(&json!("inc-42")).is_empty());
        assert_eq!(
            v.validate(&json!("INC-42")).len(),
            1,
            "wrong case: pattern only"
        );
        assert_eq!(
            v.validate(&json!("inc-123456789")).len(),
            1,
            "too long: length only"
        );
        // Both at once, and both reported — the point of collecting rather than short-circuiting.
        assert_eq!(v.validate(&json!("INC-123456789")).len(), 2);
    }

    #[test]
    fn formats_are_actually_checked() {
        let v = compile(json!({"type": "string", "format": "date-time"}));
        assert!(v.validate(&json!("2026-09-19T10:00:00Z")).is_empty());
        assert_eq!(v.validate(&json!("2026-09-19")).len(), 1);

        let d = compile(json!({"type": "string", "format": "date"}));
        assert!(d.validate(&json!("2026-09-19")).is_empty());
        assert_eq!(d.validate(&json!("19/09/2026")).len(), 1);

        let u = compile(json!({"type": "string", "format": "uuid"}));
        assert!(u
            .validate(&json!("4d0cf3a0-9a9c-4c1a-9c2a-1f2e3d4c5b6a"))
            .is_empty());
        assert_eq!(u.validate(&json!("not-a-uuid")).len(), 1);
    }

    #[test]
    fn additional_properties_false_rejects_unknown_fields() {
        let v = compile(json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {"a": {"type": "string"}},
        }));
        assert!(v.validate(&json!({"a": "x"})).is_empty());
        let errors = v.validate(&json!({"a": "x", "b": 1}));
        assert_eq!(errors, vec!["<root>: unknown field `b`"]);
    }

    #[test]
    fn nested_paths_are_reported_so_the_writer_knows_where_to_look() {
        let v = compile(json!({
            "type": "object",
            "properties": {
                "provenance": {
                    "type": "object",
                    "required": ["source"],
                    "properties": {"source": {"type": "string"}},
                },
            },
        }));
        let errors = v.validate(&json!({"provenance": {}}));
        assert_eq!(errors, vec!["provenance: missing required field `source`"]);
    }

    #[test]
    fn array_bounds_uniqueness_and_item_types() {
        let v = compile(json!({
            "type": "array",
            "minItems": 1,
            "maxItems": 2,
            "uniqueItems": true,
            "items": {"type": "string"},
        }));
        assert!(v.validate(&json!(["a"])).is_empty());
        assert_eq!(v.validate(&json!([])).len(), 1);
        assert_eq!(v.validate(&json!(["a", "a"])).len(), 1);
        assert_eq!(v.validate(&json!(["a", "b", "c"])).len(), 1);
        assert_eq!(v.validate(&json!([1])).len(), 1);
    }

    #[test]
    fn integer_and_number_are_distinguished_and_bounded() {
        let i = compile(json!({"type": "integer", "minimum": 0, "maximum": 10}));
        assert!(i.validate(&json!(5)).is_empty());
        assert_eq!(i.validate(&json!(1.5)).len(), 1);
        assert_eq!(i.validate(&json!(11)).len(), 1);

        let n = compile(json!({"type": "number"}));
        assert!(n.validate(&json!(1.5)).is_empty());
        assert!(n.validate(&json!(2)).is_empty());
    }

    #[test]
    fn enum_and_const_are_enforced() {
        let e = compile(json!({"enum": ["low", "high"]}));
        assert!(e.validate(&json!("low")).is_empty());
        assert_eq!(e.validate(&json!("medium")).len(), 1);

        let c = compile(json!({"const": "incident"}));
        assert!(c.validate(&json!("incident")).is_empty());
        assert_eq!(c.validate(&json!("decision")).len(), 1);
    }

    #[test]
    fn a_length_limit_counts_characters_not_bytes() {
        let v = compile(json!({"type": "string", "maxLength": 3}));
        assert!(v.validate(&json!("éàü")).is_empty());
    }

    // ── Fuzzing ──────────────────────────────────────────────────────────────────────
    //
    // The documents this validates are attacker-controlled (`metadata` on a memory write), so the
    // property is: whatever the document, `validate` returns. The *schemas* are ours, which is why
    // `compile` may reject — but it must reject rather than panic.

    use proptest::prelude::*;

    fn arb_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::from),
            any::<i64>().prop_map(Value::from),
            any::<f64>()
                .prop_filter("finite", |f| f.is_finite())
                .prop_map(Value::from),
            ".*".prop_map(Value::from),
        ];
        leaf.prop_recursive(4, 24, 6, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
                prop::collection::hash_map(".*", inner, 0..6)
                    .prop_map(|m| Value::Object(m.into_iter().collect())),
            ]
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(300))]

        /// Every embedded fact schema, against arbitrary documents.
        #[test]
        fn validating_arbitrary_documents_never_panics(document in arb_value()) {
            for kind in crate::memory::facts::FactKind::ALL {
                let schema: Value = serde_json::from_str(kind.schema_json()).unwrap();
                let validator = Validator::compile(&schema).unwrap();
                let _ = validator.validate(&document);
            }
        }

        /// Compiling arbitrary JSON as a schema returns — an error for anything that is not one,
        /// never a panic. `compile` runs at startup on files from disk in some deployments.
        #[test]
        fn compiling_arbitrary_json_never_panics(schema in arb_value()) {
            let _ = Validator::compile(&schema);
        }

        /// Same document, same verdict: a validator whose answer depended on map iteration order
        /// would refuse a write intermittently.
        #[test]
        fn validation_is_deterministic(document in arb_value()) {
            let schema: Value = serde_json::from_str(
                crate::memory::facts::FactKind::Incident.schema_json(),
            )
            .unwrap();
            let validator = Validator::compile(&schema).unwrap();
            prop_assert_eq!(validator.validate(&document), validator.validate(&document));
        }
    }
}
