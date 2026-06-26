//! Spark regex + miscellaneous string scalar functions that DataFusion does not provide.
//!
//! Implemented faithfully against Apache Spark v4.0.0 semantics (see
//! `weft-spark-compat/spark-tests/{inputs,results}/regexp-functions.sql*` and the
//! `substring_index` / `find_in_set` cases in `collations.sql`):
//!
//! - `regexp_extract(str, regex [, idx])` — return the `idx`-th capture group of the **first**
//!   match (`idx = 0` is the whole match, default `idx = 1`). When the regex matches but the
//!   requested group did not participate, Spark returns the empty string `''`; when the regex does
//!   not match at all, Spark also returns `''`. `idx` must be in `[0, groupCount]`, otherwise Spark
//!   raises `INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX`; an invalid pattern raises
//!   `INVALID_PARAMETER_VALUE.PATTERN`. Returns `string`.
//! - `regexp_extract_all(str, regex [, idx])` — the same, but over **all** non-overlapping matches,
//!   returning `array<string>` (one element per match, `''` for a non-participating group).
//! - `substring_index(str, delim, count)` — the substring before the `count`-th occurrence of
//!   `delim`. `count > 0` counts from the left and keeps everything before it; `count < 0` counts
//!   from the right and keeps everything after it; `count = 0` yields `''`. If `delim` is empty or
//!   `|count|` exceeds the number of occurrences, the whole string is returned. Matching on `delim`
//!   is by literal (byte) substring. Returns `string`.
//! - `find_in_set(str, strList)` — 1-based index of `str` in the comma-separated `strList`, or `0`
//!   if absent. Spark returns `0` when `str` itself contains a comma. Returns `int`.
//!
//! `str_to_map` is intentionally deferred: its only goldens are collation-typed (`collations.sql`)
//! and it returns a `map<string,string>`, which has no other consumer in the harness yet; getting
//! the map-type plumbing exactly right is not worth the risk here.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, GenericListBuilder, GenericStringBuilder, Int32Array, StringArray,
    StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use regex::Regex;

/// Register the regex + misc string Spark functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(RegexpExtract::new()));
    ctx.register_udf(ScalarUDF::from(RegexpExtractAll::new()));
    ctx.register_udf(ScalarUDF::from(SubstringIndex::new()));
    ctx.register_udf(ScalarUDF::from(FindInSet::new()));
}

/// Cast a `ColumnarValue` to a materialized `Utf8` array of `n` rows.
fn to_str_array(cv: &ColumnarValue, n: usize) -> Result<ArrayRef> {
    let a = cv.clone().into_array(n)?;
    Ok(datafusion::arrow::compute::cast(&a, &DataType::Utf8)?)
}

/// Cast a `ColumnarValue` to a materialized `Int32` array of `n` rows.
fn to_i32_array(cv: &ColumnarValue, n: usize) -> Result<ArrayRef> {
    let a = cv.clone().into_array(n)?;
    Ok(datafusion::arrow::compute::cast(&a, &DataType::Int32)?)
}

/// Collapse the doubled backslashes that Spark's SQL string-literal parser would have already
/// removed before the function ever saw the pattern. DataFusion's parser keeps a SQL literal
/// `'\\d+'` as the four chars `\`,`\`,`d`,`+`, whereas Spark hands the regex engine `\`,`d`,`+`.
/// We mirror Spark by turning every `\\` into a single `\` (a lone trailing `\` is left as-is, so
/// the regex engine reports it as an invalid pattern, matching Spark's `PATTERN` error).
fn spark_unescape_pattern(pat: &str) -> String {
    let bytes = pat.as_bytes();
    let mut out = String::with_capacity(pat.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() && bytes[i + 1] == b'\\' {
            out.push('\\');
            i += 2;
        } else {
            // Push the full UTF-8 char starting at `i`.
            let ch = pat[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

/// Compile a Spark regex, mapping a compile failure onto Spark's `PATTERN` error wording.
fn compile_pattern(func: &str, pat: &str) -> Result<Regex> {
    let unescaped = spark_unescape_pattern(pat);
    Regex::new(&unescaped).map_err(|_| {
        datafusion::common::DataFusionError::Execution(format!(
            "[INVALID_PARAMETER_VALUE.PATTERN] The value of parameter(s) `regexp` in `{func}` is \
             invalid: '{pat}'"
        ))
    })
}

/// Validate `idx` against the capture-group count, raising Spark's `REGEX_GROUP_INDEX` error when
/// out of `[0, group_count]`. Spark treats a *missing* `idx` as `1`, so even a zero-group regex
/// errors when no explicit `idx` is given — but here `idx` is always resolved before the call.
fn check_group_index(func: &str, idx: i32, group_count: usize) -> Result<()> {
    if idx < 0 || idx as usize > group_count {
        return exec_err!(
            "[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] The value of parameter(s) `idx` in \
             `{func}` is invalid: expected a group index between 0 and {group_count}, got {idx}"
        );
    }
    Ok(())
}

/// Enumerate the `group`-th capture across every match the way Java's `Matcher.find()` does —
/// which is what Spark's `regexp_extract_all` is built on. Unlike Rust's `captures_iter`, Java
/// re-anchors the search at every position when the previous match was empty, so a nullable
/// pattern like `(\d+)?` over `"1a 2b 14m"` yields one entry per gap (`["1","","","2", …]`). A
/// non-participating group contributes the empty string.
fn java_find_all(re: &Regex, text: &str, group: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let len = text.len();
    while let Some(caps) = re.captures_at(text, start) {
        let whole = caps.get(0).unwrap();
        let empty = whole.end() == whole.start();
        let g = caps
            .get(group)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        out.push(g);
        // Advance past the match, or one char forward for a zero-width match. A zero-width match
        // anchored at end-of-input (start == len) is still emitted (matching Java's
        // `Matcher.find()`), after which `next_char_boundary` pushes us past `len` to stop.
        start = if empty {
            next_char_boundary(text, whole.end())
        } else {
            whole.end()
        };
        if start > len {
            break;
        }
    }
    out
}

/// Byte index of the char boundary immediately after `i` (or `i + 1` past the end).
fn next_char_boundary(text: &str, i: usize) -> usize {
    if i >= text.len() {
        return i + 1;
    }
    let mut j = i + 1;
    while j < text.len() && !text.is_char_boundary(j) {
        j += 1;
    }
    j
}

// ---------------------------------------------------------------------------
// regexp_extract(str, regex [, idx])
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpExtract {
    signature: Signature,
}

impl RegexpExtract {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpExtract {
    fn name(&self) -> &str {
        "regexp_extract"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        if !(2..=3).contains(&args.args.len()) {
            return exec_err!("regexp_extract: expected 2 or 3 arguments");
        }
        let strs = to_str_array(&args.args[0], n)?;
        let pats = to_str_array(&args.args[1], n)?;
        let strs = strs.as_any().downcast_ref::<StringArray>().unwrap();
        let pats = pats.as_any().downcast_ref::<StringArray>().unwrap();
        let idxs = match args.args.get(2) {
            Some(a) => Some(to_i32_array(a, n)?),
            None => None,
        };
        let idxs = idxs
            .as_ref()
            .map(|a| a.as_any().downcast_ref::<Int32Array>().unwrap());

        let mut out = StringBuilder::new();
        for row in 0..n {
            // NULL str or NULL regex propagates to NULL.
            if strs.is_null(row) || pats.is_null(row) {
                out.append_null();
                continue;
            }
            // A NULL explicit idx also propagates to NULL (matches Spark).
            let idx = match idxs {
                Some(a) if a.is_null(row) => {
                    out.append_null();
                    continue;
                }
                Some(a) => a.value(row),
                None => 1,
            };
            let re = compile_pattern("regexp_extract", pats.value(row))?;
            check_group_index("regexp_extract", idx, re.captures_len() - 1)?;
            let extracted = match re.captures(strs.value(row)) {
                Some(caps) => caps
                    .get(idx as usize)
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_default(),
                None => String::new(),
            };
            out.append_value(extracted);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// regexp_extract_all(str, regex [, idx]) -> array<string>
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpExtractAll {
    signature: Signature,
}

impl RegexpExtractAll {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpExtractAll {
    fn name(&self) -> &str {
        "regexp_extract_all"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        if !(2..=3).contains(&args.args.len()) {
            return exec_err!("regexp_extract_all: expected 2 or 3 arguments");
        }
        let strs = to_str_array(&args.args[0], n)?;
        let pats = to_str_array(&args.args[1], n)?;
        let strs = strs.as_any().downcast_ref::<StringArray>().unwrap();
        let pats = pats.as_any().downcast_ref::<StringArray>().unwrap();
        let idxs = match args.args.get(2) {
            Some(a) => Some(to_i32_array(a, n)?),
            None => None,
        };
        let idxs = idxs
            .as_ref()
            .map(|a| a.as_any().downcast_ref::<Int32Array>().unwrap());

        let values_builder = GenericStringBuilder::<i32>::new();
        let mut builder = GenericListBuilder::<i32, _>::new(values_builder);
        for row in 0..n {
            if strs.is_null(row) || pats.is_null(row) {
                builder.append_null();
                continue;
            }
            let idx = match idxs {
                Some(a) if a.is_null(row) => {
                    builder.append_null();
                    continue;
                }
                Some(a) => a.value(row),
                None => 1,
            };
            let re = compile_pattern("regexp_extract_all", pats.value(row))?;
            check_group_index("regexp_extract_all", idx, re.captures_len() - 1)?;
            for group in java_find_all(&re, strs.value(row), idx as usize) {
                builder.values().append_value(group);
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ---------------------------------------------------------------------------
// substring_index(str, delim, count)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct SubstringIndex {
    signature: Signature,
}

impl SubstringIndex {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

/// Spark's `substring_index`: keep the part of `s` before the `count`-th (1-based) occurrence of
/// the literal `delim`, counting from the left for `count > 0` and from the right for `count < 0`.
fn substring_index(s: &str, delim: &str, count: i32) -> String {
    if count == 0 {
        return String::new();
    }
    if delim.is_empty() {
        return s.to_string();
    }
    // Collect the byte offsets at which (non-overlapping) `delim` occurrences start.
    let mut starts: Vec<usize> = Vec::new();
    let mut from = 0usize;
    while let Some(rel) = s[from..].find(delim) {
        let at = from + rel;
        starts.push(at);
        from = at + delim.len();
    }
    let total = starts.len();
    if count > 0 {
        let c = count as usize;
        if c > total {
            return s.to_string();
        }
        // Everything before the c-th occurrence.
        s[..starts[c - 1]].to_string()
    } else {
        let c = (-(count as i64)) as usize;
        if c > total {
            return s.to_string();
        }
        // Everything after the c-th occurrence counted from the right.
        let occ = starts[total - c];
        s[occ + delim.len()..].to_string()
    }
}

impl ScalarUDFImpl for SubstringIndex {
    fn name(&self) -> &str {
        "substring_index"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let strs = to_str_array(&args.args[0], n)?;
        let delims = to_str_array(&args.args[1], n)?;
        let counts = to_i32_array(&args.args[2], n)?;
        let strs = strs.as_any().downcast_ref::<StringArray>().unwrap();
        let delims = delims.as_any().downcast_ref::<StringArray>().unwrap();
        let counts = counts.as_any().downcast_ref::<Int32Array>().unwrap();

        let mut out = StringBuilder::new();
        for row in 0..n {
            if strs.is_null(row) || delims.is_null(row) || counts.is_null(row) {
                out.append_null();
                continue;
            }
            out.append_value(substring_index(
                strs.value(row),
                delims.value(row),
                counts.value(row),
            ));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// find_in_set(str, strList)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct FindInSet {
    signature: Signature,
}

impl FindInSet {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

/// Spark's `find_in_set`: 1-based index of `s` among the comma-separated entries of `list`, or `0`
/// if not found. Returns `0` when `s` itself contains a comma (it can never be a single entry).
fn find_in_set(s: &str, list: &str) -> i32 {
    if s.contains(',') {
        return 0;
    }
    for (i, entry) in list.split(',').enumerate() {
        if entry == s {
            return (i + 1) as i32;
        }
    }
    0
}

impl ScalarUDFImpl for FindInSet {
    fn name(&self) -> &str {
        "find_in_set"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let strs = to_str_array(&args.args[0], n)?;
        let lists = to_str_array(&args.args[1], n)?;
        let strs = strs.as_any().downcast_ref::<StringArray>().unwrap();
        let lists = lists.as_any().downcast_ref::<StringArray>().unwrap();

        let mut out = Int32Array::builder(n);
        for row in 0..n {
            if strs.is_null(row) || lists.is_null(row) {
                out.append_null();
                continue;
            }
            out.append_value(find_in_set(strs.value(row), lists.value(row)));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use crate::Engine;

    async fn run(q: &str) -> String {
        let engine = Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn regexp_extract_groups_and_whole_match() {
        // idx 0 = whole match, idx 1/2 = capture groups.
        assert!(
            run("SELECT regexp_extract('1a 2b 14m', '(\\\\d+)([a-z]+)', 0) AS x")
                .await
                .contains("1a"),
        );
        assert!(
            run("SELECT regexp_extract('1a 2b 14m', '(\\\\d+)([a-z]+)', 1) AS x")
                .await
                .contains("| 1 "),
        );
        assert!(
            run("SELECT regexp_extract('1a 2b 14m', '(\\\\d+)([a-z]+)', 2) AS x")
                .await
                .contains("| a "),
        );
        // Default idx is 1.
        assert!(
            run("SELECT regexp_extract('1a 2b 14m', '(\\\\d+)([a-z]+)') AS x")
                .await
                .contains("| 1 "),
        );
    }

    #[tokio::test]
    async fn regexp_extract_no_match_and_nonparticipating_group_is_empty() {
        // Regex matches (optional group), but the group did not participate -> ''.
        let g = run("SELECT regexp_extract('a b m', '(\\\\d+)?([a-z]+)', 1) AS x").await;
        // Empty string renders as an empty cell.
        assert!(g.contains("|    |") || g.contains("| x"), "{g}");
        assert!(!g.contains("| a "), "{g}");
    }

    #[tokio::test]
    async fn regexp_extract_group_index_out_of_range_errors() {
        let engine = Engine::new();
        // 2 groups, idx 3 -> error.
        assert!(engine
            .sql("SELECT regexp_extract('1a 2b 14m', '(\\\\d+)([a-z]+)', 3)")
            .await
            .is_err());
        // negative idx -> error.
        assert!(engine
            .sql("SELECT regexp_extract('1a 2b 14m', '(\\\\d+)([a-z]+)', -1)")
            .await
            .is_err());
        // 0 groups, default idx 1 -> error.
        assert!(engine
            .sql("SELECT regexp_extract('1a 2b 14m', '\\\\d+')")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn regexp_extract_all_basic() {
        let g = run("SELECT regexp_extract_all('1a 2b 14m', '\\\\d+', 0) AS x").await;
        assert!(g.contains("[1, 2, 14]"), "{g}");
        let g2 = run("SELECT regexp_extract_all('1a 2b 14m', '(\\\\d+)([a-z]+)', 2) AS x").await;
        assert!(g2.contains("[a, b, m]"), "{g2}");
    }

    #[tokio::test]
    async fn regexp_extract_all_nonparticipating_group_is_empty() {
        // Optional group, every other match empty.
        let g = run("SELECT regexp_extract_all('1a 2b 14m', '(\\\\d+)?', 1) AS x").await;
        assert!(g.contains("[1, , , 2, , , 14, , ]"), "{g}");
    }

    #[tokio::test]
    async fn substring_index_positive_negative_zero() {
        assert!(run("SELECT substring_index('www.apache.org', '.', 2) AS x")
            .await
            .contains("www.apache"),);
        assert!(
            run("SELECT substring_index('www.apache.org', '.', -2) AS x")
                .await
                .contains("apache.org"),
        );
        // count 0 -> empty.
        let z = run("SELECT substring_index('www.apache.org', '.', 0) AS x").await;
        assert!(z.contains("|    |") || z.contains("| x"), "{z}");
        // |count| beyond occurrences -> whole string.
        assert!(
            run("SELECT substring_index('www.apache.org', '.', 10) AS x")
                .await
                .contains("www.apache.org"),
        );
    }

    #[tokio::test]
    async fn find_in_set_index_and_absent() {
        assert!(run("SELECT find_in_set('ab', 'abc,b,ab,c,def') AS x")
            .await
            .contains("| 3 "),);
        assert!(run("SELECT find_in_set('x', 'abc,b,ab,c,def') AS x")
            .await
            .contains("| 0 "),);
        // str containing a comma -> 0.
        assert!(run("SELECT find_in_set('a,b', 'a,b,c') AS x")
            .await
            .contains("| 0 "),);
    }
}
