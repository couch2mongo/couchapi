use crate::ops::get::ViewOptions;
use crate::ops::JsonWithStatusCodeResponse;
use axum::http::StatusCode;
use axum::Json;
use boa_engine::property::Attribute;
use boa_engine::{Context, JsValue, Source};
use boa_runtime::Console;
use bson::Document;
use serde_json::{json, Value};
use std::panic;
use tracing::warn;

pub fn execute_script(
    source_file: &str,
    view_options: &ViewOptions,
) -> Result<Vec<Document>, JsonWithStatusCodeResponse> {
    warn!(
        source_file = source_file,
        "** BREAK GLASS ** execute_script"
    );

    let script_source = std::fs::read_to_string(source_file).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
    })?;

    inner_execute_script(&script_source, view_options)
}

fn inner_execute_script(
    script: &str,
    view_options: &ViewOptions,
) -> Result<Vec<Document>, JsonWithStatusCodeResponse> {
    let mut context = Context::default();

    let console = Console::init(&mut context);
    context
        .register_global_property(Console::NAME, console, Attribute::all())
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
        })?;

    // convert view_options to a serde_json::Value
    let view_options_value = serde_json::to_value(view_options).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
    })?;

    let view_options_js = JsValue::from_json(&view_options_value, &mut context).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
    })?;

    context
        .register_global_property("view_options", view_options_js, Attribute::all())
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
        })?;

    let src = Source::from_bytes(script.as_bytes());

    context.eval(src).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
    })?;

    let result = context
        .global_object()
        .get("result", &mut context)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
        })?;

    let json = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        result.to_json(&mut context).unwrap()
    })).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!(
                {
                    "error": "chances are a bug in the script resulted in an undefined appearing the result variable."
                }
            )),
        )
    })?;

    let return_value_vector = if let Value::Array(v) = json {
        v
    } else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "return value is not an array"})),
        ));
    };

    // Convert return_value_vector to a Vec<Document>
    let return_value_docs: Vec<Document> = return_value_vector
        .iter()
        .map(|v| bson::to_document(v).unwrap())
        .collect();

    Ok(return_value_docs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::get::ViewOptions;

    #[tokio::test]
    async fn execute_script_returns_a_document() {
        let view_options = ViewOptions {
            reduce: false,
            group: false,
            group_level: 0,
            include_docs: false,
            descending: false,
            limit: None,
            skip: 0,
            start_key: vec![],
            end_key: vec![],
            startkey_docid: None,
            endkey_docid: None,
            keys: vec![],
        };

        let script = r#"
            function main(params) {
                return [
                     {
                        $sort: {
                            "date": 1
                    }
                }];
            }

            result = main(view_options)"#;

        let result = inner_execute_script(script, &view_options).unwrap();

        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn execute_script_returns_an_undefined() {
        let view_options = ViewOptions {
            reduce: false,
            group: false,
            group_level: 0,
            include_docs: false,
            descending: false,
            limit: None,
            skip: 0,
            start_key: vec![],
            end_key: vec![],
            startkey_docid: None,
            endkey_docid: None,
            keys: vec![],
        };

        let script = r#"
            function main(params) {
                return [
                     {
                        $sort: {
                            "date": undefined
                    }
                }];
            }

            result = main(view_options)"#;

        let result = inner_execute_script(script, &view_options);

        assert!(result.is_err());
    }
}
