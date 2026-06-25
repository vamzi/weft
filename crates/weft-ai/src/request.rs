//! Builds the Anthropic Messages API request for an [`AssistRequest`](crate::AssistRequest).
//!
//! The model is grounded on the (governed) catalog schema and constrained to structured output via
//! `output_config.format` (a JSON Schema), so the returned content is a well-formed [`Generation`]
//! by construction rather than free text to parse. This is the pure request-construction half of a
//! live [`AiProvider`](crate::AiProvider); only the HTTP transport + API-key wiring (from Secrets
//! Manager) remain.
//!
//! See the `claude-api` reference: default model [`DEFAULT_MODEL`](crate::DEFAULT_MODEL), Messages
//! API with `output_config.format`, adaptive thinking. Streaming is used at the transport layer for
//! live token preview.

use serde_json::{json, Value};

use crate::{AssistRequest, Intent, DEFAULT_MODEL, NOTEBOOK_OUTPUT_SCHEMA, SQL_OUTPUT_SCHEMA};

/// The default output-token ceiling for a generation (an editor statement / a notebook fit well
/// within this; bump for very large notebooks).
const MAX_TOKENS: u32 = 4096;

/// The JSON Schema the model's output is constrained to for a given intent.
fn output_schema(intent: Intent) -> Value {
    let schema_str = match intent {
        Intent::GenerateNotebook => NOTEBOOK_OUTPUT_SCHEMA,
        // SQL editor intents all produce `{ "sql": ... }`.
        Intent::GenerateSql | Intent::Explain | Intent::Fix | Intent::Optimize => SQL_OUTPUT_SCHEMA,
    };
    serde_json::from_str(schema_str).expect("embedded output schema is valid JSON")
}

/// The task instruction for an intent, prepended to the user's prompt.
fn instruction(intent: Intent) -> &'static str {
    match intent {
        Intent::GenerateSql => "Write a single SQL statement that answers the request.",
        Intent::GenerateNotebook => {
            "Produce an ordered list of notebook cells (markdown, sql, python) that accomplish the request."
        }
        Intent::Explain => "Explain what the given SQL does; return the explanation as a SQL comment.",
        Intent::Fix => "Fix the error in the given SQL and return the corrected statement.",
        Intent::Optimize => "Rewrite the given SQL to be more efficient, preserving its results.",
    }
}

/// Build the Messages API request body (as JSON) for `req`.
///
/// Grounds the model on `req.catalog_context` (the governed schema slice) via the system prompt and
/// constrains the response to the intent's JSON Schema. The caller adds auth headers and POSTs to
/// `/v1/messages` (streaming).
pub fn build_request(req: &AssistRequest) -> Value {
    let system = format!(
        "You are a data analyst's assistant for the Weft platform. Generate correct, executable \
         Spark SQL / PySpark grounded ONLY in the catalog schema provided. Do not reference tables \
         or columns that are not in the schema.\n\nAvailable catalog schema:\n{}",
        if req.catalog_context.trim().is_empty() {
            "(no schema provided)"
        } else {
            req.catalog_context.as_str()
        }
    );

    json!({
        "model": DEFAULT_MODEL,
        "max_tokens": MAX_TOKENS,
        "thinking": { "type": "adaptive" },
        "system": system,
        "messages": [
            { "role": "user", "content": format!("{}\n\n{}", instruction(req.intent), req.prompt) }
        ],
        "output_config": {
            "format": { "type": "json_schema", "schema": output_schema(req.intent) }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(intent: Intent) -> AssistRequest {
        AssistRequest {
            prompt: "top 10 customers by revenue".into(),
            intent,
            catalog_context: "TABLE main.sales.orders(customer_id BIGINT, amount DOUBLE)".into(),
        }
    }

    #[test]
    fn builds_sql_request_with_schema_constraint() {
        let body = build_request(&req(Intent::GenerateSql));
        assert_eq!(body["model"], DEFAULT_MODEL);
        // Output is constrained to the SQL JSON schema (requires the `sql` property).
        let schema = &body["output_config"]["format"]["schema"];
        assert_eq!(body["output_config"]["format"]["type"], "json_schema");
        assert!(schema["properties"].get("sql").is_some());
        // Grounding: the catalog schema is in the system prompt; the prompt is in the user turn.
        assert!(body["system"]
            .as_str()
            .unwrap()
            .contains("main.sales.orders"));
        assert!(body["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("top 10 customers"));
    }

    #[test]
    fn notebook_intent_uses_cells_schema() {
        let body = build_request(&req(Intent::GenerateNotebook));
        let schema = &body["output_config"]["format"]["schema"];
        assert!(schema["properties"].get("cells").is_some());
    }

    #[test]
    fn empty_context_is_handled() {
        let mut r = req(Intent::GenerateSql);
        r.catalog_context = String::new();
        let body = build_request(&r);
        assert!(body["system"]
            .as_str()
            .unwrap()
            .contains("no schema provided"));
    }
}
