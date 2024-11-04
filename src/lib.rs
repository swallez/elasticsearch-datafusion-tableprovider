//! A [DataFusion table provider](https://datafusion.apache.org/library-user-guide/custom-table-providers.html)
//! that loads Elasticsearch indexes as DataFusion tables, and transforms DataFusion queries to
//! [ES|QL](https://www.elastic.co/guide/en/elasticsearch/reference/master/esql.html) queries.
//!
//! **Note**: this is an experiment, mapping from Elasticsearch types to DataFusion is incomplete, and
//! there is some plan optimizations to be done to push more processing down to Elasticsearch.
//!
//! Use it as follows:
//! ```text
//! CREATE EXTERNAL TABLE some_index STORED AS ELASTICSEARCH LOCATION <url> OPTIONS(<options>)
//! ```
//!
//! Where `<url>` can be either a http URL, or an `env:<ES_NAME>` URL that will cause the table
//! configuration to be read from environment variables.
//!
//! When using a http URL, the following options can be provided to configure authentication:
//! * `api_key`: use API key authentication,
//! * `token`: use bearer token authentication,
//! * `user` & `password`: basic authentication.
//!
//! When using an env url, the configuration is read from environment variables. Assuming `env:PRODUCTS` url:
//! * `ELASTICSEARCH_PRODUCTS_URL`: Elasticsearch server URL,
//! * `ELASTICSEARCH_PRODUCTS_API_KEY`: API key authentication (other schemes aren't implemented yet for env URLs)
//!

mod es_types;
mod utils;
mod expr;

use std::any::Any;
use std::env;
use std::fmt::{Formatter, Pointer};
use std::io::{Cursor, Read};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::common::{not_impl_err, plan_err, project_schema, ColumnStatistics, Constraint, Constraints, Statistics};
use datafusion::common::stats::Precision;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{CreateExternalTable, Expr, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties};
use datafusion::physical_plan::insert::{DataSink, DataSinkExec};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use elasticsearch::Elasticsearch;
use elasticsearch::indices::IndicesGetMappingParts;
use futures::{Stream, StreamExt};
use crate::es_types::MappingResponse;
use itertools::Itertools;
use serde_json::json;
use url::Url;
use crate::utils::convert_error;

//-------------------------------------------------------------------------------------------------
// ElasticsearchTableProviderFactory

pub struct ElasticsearchTableProviderFactory {
}

impl ElasticsearchTableProviderFactory {

    pub fn new() -> Self {
        ElasticsearchTableProviderFactory{}
    }

    /// Register an `"ELASTICSEARCH"` table type in a `SessionContext`.
    pub fn register(ctx: &SessionContext) {
        ctx.state_ref().write().table_factories_mut().insert(
            "ELASTICSEARCH".into(),
            Arc::new(ElasticsearchTableProviderFactory::new()),
        );

        // DataFusion tries to find an ObjectStore implementation for every URL. So register a dummy
        // store for the "env:" scheme that we used to read the configuratin from environment variables.
        let env_url = Url::parse("env:").unwrap();
        ctx.register_object_store(&env_url, Arc::new(object_store::memory::InMemory::new()));
    }

    async fn read_schema(&self, client: &Elasticsearch, index: &str, expose_id: bool) -> datafusion::error::Result<SchemaRef> {
        let response = client.indices().get_mapping(IndicesGetMappingParts::Index(&[index]))
            .send().await
            .map_err(convert_error)?;

        if !response.status_code().is_success() {
            return Err(DataFusionError::Execution(
                format!("Error reading '{index}' mappings ({})", response.status_code().as_u16())
            ));
        }

        //let bytes = response.text().await.map_err(convert_error)?;
        //let mut mappings: MappingResponse = serde_json::from_str(&bytes).map_err(convert_error)?;

        let mut mappings: MappingResponse = response.json().await.map_err(convert_error)?;

        let mapping = mappings.remove(index)
            .ok_or_else(|| DataFusionError::Execution(format!("Mapping for index {} not found", index)))?;

        let mut fields = Vec::<Field>::new();

        if expose_id {
            fields.push(Field::new("_id", DataType::Utf8, false));
        }

        for (name, prop) in mapping.mappings.properties {
            let arrow_type = match prop.type_.as_str() {
                // FIXME: to be refined
                "null" => DataType::Null,
                "boolean" => DataType::Boolean,

                "byte" => DataType::UInt8,
                "short" => DataType::Int16,
                "integer" => DataType::Int32,
                "long" => DataType::Int64,

                "float" => DataType::Float32,
                "double" => DataType::Float64,

                "text" | "match_only_text" => DataType::Utf8,
                "keyword" | "constant_keyword" | "wildcard" => DataType::Utf8,

                "date" => DataType::Timestamp(TimeUnit::Millisecond, None),
                "ip" => DataType::Binary,
                "version" => DataType::Utf8,

                _ => return Err(DataFusionError::Execution(format!("Unsupported field type {}", prop.type_))),
            };

            fields.push(Field::new(name, arrow_type, true));

        }

        println!("Elasticsearch index '{}' has {} fields.", index, &fields.len());

        let schema = Schema::new(fields);
        Ok(Arc::new(schema))
    }
}

#[async_trait]
impl TableProviderFactory for ElasticsearchTableProviderFactory {

    async fn create(&self, _state: &dyn Session, cmd: &CreateExternalTable) -> datafusion::common::Result<Arc<dyn TableProvider>> {

        use elasticsearch::auth::Credentials;

        //println!("Cmd = {:?}", cmd);

        let mut location = &cmd.location;
        let mut creds: Option<Credentials> = None;

        let env_str;
        if let Some(name) = location.strip_prefix("env:") {
            env_str = env::var(format!("ELASTICSEARCH_{name}_URL"))
                .map_err(convert_error)?;
            location = &env_str;

            creds = Some(Credentials::EncodedApiKey(
                env::var(format!("ELASTICSEARCH_{name}_API_KEY"))
                    .map_err(convert_error)?
            ));
        }

        let url = url::Url::parse(location)
            .or_else(|_| Err(DataFusionError::Configuration(format!("Invalid URL {}", &cmd.location))))?;

        //---- Credentials
        // TODO: read credentials from a config file.
        let creds = if let Some(creds) = creds {
            // Defined in the env file
            Some(creds)
        }
        else if let Some(api_key) = cmd.options.get("api-key") {
            Some(Credentials::EncodedApiKey(api_key.to_string()))

        } else if let Some(token) = cmd.options.get("token") {
            Some(Credentials::Bearer(token.to_string()))

        } else if let Some(user) = cmd.options.get("user") {
            if let Some(pwd) = cmd.options.get("password") {
                Some(Credentials::Basic(user.to_string(), pwd.to_string()))
            } else {
                return Err(DataFusionError::Configuration("Missing 'password' configuration".to_string()));
            }
        } else {
            None
        };

        let expose_id = true;

        // We always add the document `_id`, which is the primary key, as the first column in the schema.
        let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]);

        let index: Arc<str> = cmd.name.table().into();

        //---- Create the transport
        let pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(url);
        let mut transport = elasticsearch::http::transport::TransportBuilder::new(pool);
        if let Some(creds) = creds {
            transport = transport.auth(creds);
        }
        let transport = transport.build()
            .or_else(|e| Err(DataFusionError::External(Box::new(e))))?;

        let client = Elasticsearch::new(transport);

        //---- And put everything together
        let schema = self.read_schema(&client, &index, expose_id).await?;

        let table = ElasticsearchTableProvider {
            client: Arc::new(client),
            constraints,
            schema,
            index: index.clone()
        };

        Ok(Arc::new(table))
    }
}

//-------------------------------------------------------------------------------------------------
// ElasticsearchTableProvider

struct ElasticsearchTableProvider {
    client: Arc<Elasticsearch>,
    index: Arc<str>,
    schema: SchemaRef,
    constraints: Constraints,
}

// A `TableProvider` is a bridge to a single table's data. It has to be understood as a provider
// for the _data_ that is in a table.
#[async_trait]
impl TableProvider for ElasticsearchTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }


    fn table_type(&self) -> TableType {
        // An ES index is an ordinary table
        TableType::Base
    }

    async fn scan(&self, _state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {

        // The execution plan's schema should only contain fields that are part of the projection
        let plan_schema = project_schema(&self.schema, projection)?;

        // Build the FROM and KEEP statements
        let keep = match projection {
            Some(projection) if !projection.is_empty() => {
                let defs: &[FieldRef] = self.schema.fields.deref();
                Some(projection.iter().map(|i| defs[*i].name()).join(", "))
            }
            _ => None,
        };

        let mut esql = "FROM ".to_string();
        esql.push_str(&self.index);
        esql.push_str(" ");

        // Add _id if it's part of the projection.
        if projection.unwrap_or(&Vec::new()).contains(&0) {
            esql.push_str("METADATA _id ");
        }

        if let Some(keep) = keep {
            esql.push_str("| KEEP ");
            esql.push_str(&keep);
        }

        //---- build filters
        for expr in filters {
            esql.push_str(" | WHERE ");
            expr::add_expr(expr, 0, &mut esql)?;
        }

        if let Some(limit) = limit {
            esql.push_str(&format!(" | LIMIT {limit}"));
        }

        println!("ES|QL query: {}", esql);

        Ok(Arc::new(ElasticsearchExecutionPlan::new(self.client.clone(), esql, plan_schema)))
    }

    // -------

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }


    fn get_table_definition(&self) -> Option<&str> {
        // Cannot create an ES index from a DDL statement
        None
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        None
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        // Should we use the field's null_value?
        // See https://www.elastic.co/guide/en/elasticsearch/reference/current/null-value.html
        None
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        // FIXME: can be more efficient. We translate the predicates, which will be done again in `scan`
        //println!("Filter pushdown {:?}", filters);
        let mut esql = String::new();
        let mut result = Vec::new();
        for filter in filters {
            esql.clear();
            result.push(match expr::add_expr(filter, 0, &mut esql) {
                Ok(_) => TableProviderFilterPushDown::Exact,
                Err(e) => {
                    println!("ES: Unsupported filter: {:?}", e);
                    TableProviderFilterPushDown::Unsupported
                },
            });
        }

        Ok(result)
    }

    fn statistics(&self) -> Option<Statistics> {
        // FIXME: get a start-time snapshot of stats using GET <index>/_stats/docs,store

        let stats = self.schema.fields.as_ref().iter().map(|_| ColumnStatistics {
            null_count: Precision::Exact(20),
            max_value: Precision::Absent,
            min_value: Precision::Absent,
            distinct_count: Precision::Exact(10000),
        }).collect();

        Some(Statistics {
            num_rows: Precision::Exact(100000),
            column_statistics: stats,
            total_byte_size: Precision::Exact(1000000),
        })
    }

    async fn insert_into(&self, _state: &dyn Session, input: Arc<dyn ExecutionPlan>, _overwrite: bool) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        #[derive(Debug)]
        struct EsDataSink {
            client: Arc<Elasticsearch>,
            index: Arc<str>,
        }

        impl EsDataSink {
            async fn send_batch(&self, data: RecordBatch) -> DataFusionResult<u64> {
                let _schema = data.schema();
                let _client = &self.client;
                let _index = &self.index;
                not_impl_err!("Here we must send a bulk request to Elasticsearch")
                //Ok(0)
            }
        }

        impl DisplayAs for EsDataSink {
            fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
                self.fmt(f)
            }
        }

        #[async_trait]
        impl DataSink for EsDataSink {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn metrics(&self) -> Option<MetricsSet> {
                None
            }

            async fn write_all(&self, mut data: SendableRecordBatchStream, _context: &Arc<TaskContext>) -> DataFusionResult<u64> {
                let mut sum = 0;
                while let Some(batch) = data.next().await {
                    sum = sum + self.send_batch(batch?).await?;
                }
                Ok(sum)
            }
        }

        let sink = EsDataSink {
            client: self.client.clone(),
            index: self.index.clone(),
        };

        let sink_exec = DataSinkExec::new(input, Arc::new(sink), self.schema.clone(), None);

        Ok(Arc::new(sink_exec))
    }
}

//-------------------------------------------------------------------------------------------------
// ElasticsearchExecutionPlan
//
// The plan's schema is the table's schema with only those fields included in the projection.
// We must ensure that record batches read from the server match this schema, by verifying that
// all fields that the plan expects are present, and by coercing value types if needed
// (coercion should be avoided if possible as it can have performance implications).

#[derive(Debug)]
struct ElasticsearchExecutionPlan {
    properties: PlanProperties,
    client: Arc<Elasticsearch>,
    query: String,
    schema: SchemaRef,
}

impl ElasticsearchExecutionPlan {
    pub fn new(client: Arc<Elasticsearch>, query: String, schema: SchemaRef) -> Self {
        ElasticsearchExecutionPlan {
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
            client,
            query,
            schema,
        }
    }
}

impl DisplayAs for ElasticsearchExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Elasticsearch({})", &self.query)
    }
}

impl ExecutionPlan for ElasticsearchExecutionPlan {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // "The returned list will be empty for leaf nodes such as scans"
        Vec::new()
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            plan_err!("Children cannot be created")
        }
    }

    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> DataFusionResult<SendableRecordBatchStream> {

        use futures::TryStreamExt;

        // RecordBatch stream that will be coerced to the plan's schema.
        struct Batch<R: Read> {
            ipc_reader: StreamReader<R>,
            schema: SchemaRef,
        }

        impl <R: Read> Unpin for Batch<R> {}

        impl <R:Read> Stream for Batch<R> {
            type Item = DataFusionResult<RecordBatch>;

            fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
                let result = self.ipc_reader.next();
                let result = result.map(|r| r
                    // Enforce schema and cast if needed
                    .and_then(|batch| utils::enforce_schema(batch, &self.schema).into())
                    // Adapt error result
                    .map_err(|e| e.into())
                );
                Poll::Ready(result)
            }
        }

        impl <R:Read> RecordBatchStream for Batch<R> {
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }
        }

        async fn get_batch(client: Arc<Elasticsearch>, query: String, schema: SchemaRef) -> DataFusionResult<SendableRecordBatchStream> {
            let res = client.esql().query().
                format("arrow")
                .body(json!({
                    "query": query,
                }))
                .send().await
                .map_err(convert_error)?;

            let status_code = res.status_code();
            if !status_code.is_success() {
                let data = res.text().await.map_err(convert_error)?;
                return Err(DataFusionError::Execution(format!("ES|QL error ({}), {data}", status_code.as_u16())));
            }

            let data = res.bytes().await
                .map_err(convert_error)?;

            // Make it a `Read` that owns the data
            let data = Cursor::new(data);
            let ipc_reader = StreamReader::try_new(data, None)?;

            let stream = Batch { ipc_reader, schema };

            Ok(Box::pin(stream))
        }

        let fut = get_batch(self.client.clone(), self.query.clone(), self.schema.clone());

        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream)))
    }
}
