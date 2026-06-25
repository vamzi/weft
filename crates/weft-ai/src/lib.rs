//! `weft-ai` — AI assistance for the SQL editor and notebooks.
//!
//! Turns a natural-language prompt into either a single SQL statement (editor) or an ordered list
//! of typed notebook cells, by calling **Claude (`claude-opus-4-8`) via the Anthropic Messages
//! API** with the relevant (governed) catalog schema as grounding context and **strict tool use /
//! `output_config.format`** so the model returns *structured* output — not free text to parse.
//!
//! Two invariants the rest of the platform relies on:
//! - **Provider seam.** All LLM calls live behind [`AiProvider`]; the model/provider is swappable
//!   and the API key never leaves the backend (Secrets Manager).
//! - **Governance-aware.** Generated SQL is *suggested*, never auto-run: it still flows through the
//!   `weft-connect` authorizer, so the assistant cannot read past a user's grants.
//!
//! This module freezes the **output contract** (the shapes + their JSON Schemas) ahead of the SDK
//! wiring, so the gateway and the web AI panel build against a stable structure.

pub mod request;

/// The default model for AI assist. Latest, most capable Claude — see the platform plan.
pub const DEFAULT_MODEL: &str = "claude-opus-4-8";

/// The kind of a generated notebook cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellKind {
    /// A SQL cell.
    Sql,
    /// A Python (PySpark) cell.
    Python,
    /// A Markdown cell.
    Markdown,
}

impl CellKind {
    /// The wire string used in the structured output / notebook JSON.
    pub fn as_str(&self) -> &'static str {
        match self {
            CellKind::Sql => "sql",
            CellKind::Python => "python",
            CellKind::Markdown => "markdown",
        }
    }
}

/// One generated notebook cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedCell {
    /// Cell type.
    pub kind: CellKind,
    /// Cell source text.
    pub source: String,
}

/// The structured result of an AI generation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Generation {
    /// A single SQL statement for the editor.
    Sql(String),
    /// An ordered list of typed cells for a notebook.
    Notebook(Vec<GeneratedCell>),
}

/// What the user asked the assistant to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Intent {
    /// Generate SQL for the editor.
    GenerateSql,
    /// Generate a notebook (SQL + Python cells).
    GenerateNotebook,
    /// Explain a selected query.
    Explain,
    /// Fix an errored query/cell.
    Fix,
    /// Optimize a selected query.
    Optimize,
}

/// A request to the assistant. `catalog_context` is the governed schema slice (only objects the
/// caller may see) the model is grounded on.
#[derive(Debug, Clone)]
pub struct AssistRequest {
    /// The user's natural-language prompt.
    pub prompt: String,
    /// What to produce.
    pub intent: Intent,
    /// Governed catalog schema (DDL-ish) the model may reference.
    pub catalog_context: String,
}

/// The provider seam. The Anthropic-backed implementation lands behind this once the SDK is wired;
/// tests use a stub.
pub trait AiProvider {
    /// Generate a structured result for the request. Implementations call the Messages API with a
    /// JSON-schema-constrained output so the returned [`Generation`] is well-formed by construction.
    fn generate(&self, req: &AssistRequest) -> weft_common::Result<Generation>;
}

/// JSON Schema (as a string) the editor's NL→SQL output is constrained to: `{ "sql": "<string>" }`.
pub const SQL_OUTPUT_SCHEMA: &str = r#"{
  "type": "object",
  "additionalProperties": false,
  "required": ["sql"],
  "properties": { "sql": { "type": "string", "description": "A single executable SQL statement." } }
}"#;

/// JSON Schema (as a string) the NL→notebook output is constrained to: an ordered list of typed cells.
pub const NOTEBOOK_OUTPUT_SCHEMA: &str = r#"{
  "type": "object",
  "additionalProperties": false,
  "required": ["cells"],
  "properties": {
    "cells": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["kind", "source"],
        "properties": {
          "kind": { "type": "string", "enum": ["sql", "python", "markdown"] },
          "source": { "type": "string" }
        }
      }
    }
  }
}"#;

#[cfg(test)]
mod tests {
    use super::*;

    struct StubProvider;
    impl AiProvider for StubProvider {
        fn generate(&self, req: &AssistRequest) -> weft_common::Result<Generation> {
            Ok(match req.intent {
                Intent::GenerateNotebook => Generation::Notebook(vec![
                    GeneratedCell {
                        kind: CellKind::Markdown,
                        source: "# Analysis".into(),
                    },
                    GeneratedCell {
                        kind: CellKind::Sql,
                        source: "SELECT 1".into(),
                    },
                ]),
                _ => Generation::Sql("SELECT 1".into()),
            })
        }
    }

    #[test]
    fn stub_returns_structured_generation() {
        let p = StubProvider;
        let sql = p
            .generate(&AssistRequest {
                prompt: "count rows".into(),
                intent: Intent::GenerateSql,
                catalog_context: String::new(),
            })
            .unwrap();
        assert_eq!(sql, Generation::Sql("SELECT 1".into()));

        let nb = p
            .generate(&AssistRequest {
                prompt: "analyze sales".into(),
                intent: Intent::GenerateNotebook,
                catalog_context: String::new(),
            })
            .unwrap();
        match nb {
            Generation::Notebook(cells) => {
                assert_eq!(cells.len(), 2);
                assert_eq!(cells[1].kind.as_str(), "sql");
            }
            _ => panic!("expected notebook"),
        }
    }

    #[test]
    fn schemas_are_valid_json() {
        // Cheap structural check without a JSON dependency: balanced braces + key presence.
        assert!(SQL_OUTPUT_SCHEMA.contains("\"sql\""));
        assert!(NOTEBOOK_OUTPUT_SCHEMA.contains("\"cells\""));
        for schema in [SQL_OUTPUT_SCHEMA, NOTEBOOK_OUTPUT_SCHEMA] {
            let opens = schema.matches('{').count();
            let closes = schema.matches('}').count();
            assert_eq!(opens, closes, "unbalanced braces in schema");
        }
    }
}
