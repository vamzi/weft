//! PySpark-shaped requests over gRPC: the `SqlCommand` path (`spark.sql(...)`), the
//! `SqlCommandResult` → relation round-trip, eager DDL, and `AnalyzePlan(Schema)`. These are the
//! request shapes stock PySpark 4.x sends (vs. the raw `Sql`-relation our Rust bench client uses).

use std::time::Duration;

use sc::spark_connect_service_client::SparkConnectServiceClient;
use tonic::transport::Channel;
use weft_connect::{serve, ServerConfig};
use weft_loom::arrow::array::Int64Array;
use weft_loom::arrow::ipc::reader::StreamReader;
use weft_proto::spark::connect as sc;

const SESSION: &str = "00112233-4455-6677-8899-aabbccddeeff";

async fn boot(port: u16) -> SparkConnectServiceClient<Channel> {
    tokio::spawn(async move {
        let _ = serve(ServerConfig { port }).await;
    });
    let endpoint = format!("http://127.0.0.1:{port}");
    for _ in 0..50 {
        if let Ok(c) = SparkConnectServiceClient::connect(endpoint.clone()).await {
            return c.max_decoding_message_size(256 * 1024 * 1024);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server did not become ready on {port}");
}

fn sql_relation(query: &str) -> sc::Relation {
    sc::Relation {
        common: None,
        rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
            query: query.to_string(),
            ..Default::default()
        })),
    }
}

fn root(rel: sc::Relation) -> sc::Plan {
    sc::Plan {
        op_type: Some(sc::plan::OpType::Root(rel)),
    }
}

/// PySpark `spark.sql(q)` shape: a Command wrapping a SqlCommand whose `input` is a Sql relation.
fn sql_command(query: &str) -> sc::Plan {
    sc::Plan {
        op_type: Some(sc::plan::OpType::Command(sc::Command {
            command_type: Some(sc::command::CommandType::SqlCommand(sc::SqlCommand {
                input: Some(sql_relation(query)),
                ..Default::default()
            })),
        })),
    }
}

async fn run(
    client: &mut SparkConnectServiceClient<Channel>,
    plan: sc::Plan,
) -> Vec<sc::execute_plan_response::ResponseType> {
    let req = sc::ExecutePlanRequest {
        session_id: SESSION.to_string(),
        plan: Some(plan),
        ..Default::default()
    };
    let mut stream = client
        .execute_plan(req)
        .await
        .expect("execute_plan")
        .into_inner();
    let mut out = Vec::new();
    while let Some(msg) = stream.message().await.expect("stream") {
        if let Some(rt) = msg.response_type {
            out.push(rt);
        }
    }
    out
}

/// First i64 value across any ArrowBatch in the responses.
fn first_i64(resps: &[sc::execute_plan_response::ResponseType]) -> Option<i64> {
    for r in resps {
        if let sc::execute_plan_response::ResponseType::ArrowBatch(b) = r {
            let reader = StreamReader::try_new(std::io::Cursor::new(b.data.clone()), None).ok()?;
            for rb in reader {
                let rb = rb.ok()?;
                if let Some(c) = rb.column(0).as_any().downcast_ref::<Int64Array>() {
                    if !c.is_empty() {
                        return Some(c.value(0));
                    }
                }
            }
        }
    }
    None
}

#[tokio::test]
async fn sql_command_query_returns_lazy_relation_then_executes() {
    let mut client = boot(50591).await;

    // spark.sql("SELECT 7 AS x") → a SqlCommandResult carrying a relation handle.
    let resps = run(&mut client, sql_command("SELECT 7 AS x")).await;
    let relation = resps.iter().find_map(|r| match r {
        sc::execute_plan_response::ResponseType::SqlCommandResult(s) => s.relation.clone(),
        _ => None,
    });
    let relation = relation.expect("SqlCommandResult.relation present");
    assert!(
        resps.iter().any(|r| matches!(
            r,
            sc::execute_plan_response::ResponseType::ResultComplete(_)
        )),
        "stream must terminate with ResultComplete"
    );

    // PySpark then executes the DataFrame's relation (the `.show()`/`.collect()` step).
    let resps2 = run(&mut client, root(relation)).await;
    assert_eq!(
        first_i64(&resps2),
        Some(7),
        "executing the returned relation yields the row"
    );
}

#[tokio::test]
async fn sql_command_ddl_executes_eagerly() {
    let mut client = boot(50592).await;

    // spark.sql("CREATE TABLE ...") must run eagerly (side effect), returning a result relation.
    let resps = run(&mut client, sql_command("CREATE TABLE t AS SELECT 42 AS v")).await;
    assert!(
        resps.iter().any(|r| matches!(
            r,
            sc::execute_plan_response::ResponseType::SqlCommandResult(_)
        )),
        "a command returns a SqlCommandResult"
    );

    // The table must now exist (proves eager execution) — query it via a raw Sql relation.
    let resps2 = run(&mut client, root(sql_relation("SELECT v FROM t"))).await;
    assert_eq!(
        first_i64(&resps2),
        Some(42),
        "the eagerly-created table is queryable"
    );
}

#[tokio::test]
async fn analyze_schema_returns_spark_types() {
    let mut client = boot(50593).await;

    let req = sc::AnalyzePlanRequest {
        session_id: SESSION.to_string(),
        analyze: Some(sc::analyze_plan_request::Analyze::Schema(
            sc::analyze_plan_request::Schema {
                plan: Some(root(sql_relation("SELECT 1 AS a, 'x' AS b"))),
            },
        )),
        ..Default::default()
    };
    let resp = client
        .analyze_plan(req)
        .await
        .expect("analyze_plan")
        .into_inner();
    let sc::analyze_plan_response::Result::Schema(schema) = resp.result.expect("a result") else {
        panic!("expected Schema result")
    };
    let dt = schema.schema.expect("a DataType");
    let Some(sc::data_type::Kind::Struct(st)) = dt.kind else {
        panic!("expected struct schema")
    };
    assert_eq!(st.fields.len(), 2);
    assert_eq!(st.fields[0].name, "a");
    assert!(matches!(
        st.fields[0].data_type.as_ref().unwrap().kind,
        Some(sc::data_type::Kind::Long(_))
    ));
    assert_eq!(st.fields[1].name, "b");
    assert!(matches!(
        st.fields[1].data_type.as_ref().unwrap().kind,
        Some(sc::data_type::Kind::String(_))
    ));
}
