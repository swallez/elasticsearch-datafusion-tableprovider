use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::{
    postgres::PostgresTableFactory,
    sql::db_connection_pool::postgrespool::PostgresConnectionPool,
    util::secrets::to_secret_map,
};

pub fn register(ctx: &SessionContext) {

    ctx.state_ref().write().table_factories_mut().insert(
        "POSTGRES".into(),
        Arc::new(PgTableProviderFactory::new()),
    );
}

#[derive(Debug)]
pub struct PgTableProviderFactory {
}

impl PgTableProviderFactory {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl TableProviderFactory for PgTableProviderFactory {

    async fn create(&self, _state: &dyn Session, cmd: &CreateExternalTable) -> datafusion::common::Result<Arc<dyn TableProvider>> {

        let location = &cmd.location;
        let pg_url: &str = if let Some(name) = location.strip_prefix("env:") {
            let env_name = format!("POSTGRES_{name}_URL");
            &env::var(&env_name)
                .map_err(|_| DataFusionError::Configuration(format!("Environment variable {env_name} not found")))?
        } else {
            location
        };

        let pg_url = url::Url::parse(&pg_url)
            .map_err(|e| DataFusionError::External(e.into()))?;

        let postgres_params = to_secret_map(HashMap::from([
            ("host".to_string(), pg_url.host().map(|h| h.to_string()).unwrap_or("localhsot".to_string())),
            ("user".to_string(), pg_url.username().to_string()),
            ("db".to_string(), pg_url.path().trim_start_matches("/").to_string()),
            ("pass".to_string(), pg_url.password().unwrap_or("").to_string()),
            ("port".to_string(), pg_url.port().unwrap_or(5432).to_string()),
            ("sslmode".to_string(), "disable".to_string()),
        ]));

        // Create Postgres connection pool
        let postgres_pool = Arc::new(
            PostgresConnectionPool::new(postgres_params)
                .await
                .expect("unable to create PostgreSQL connection pool"),
        );

        Ok(PostgresTableFactory::new(postgres_pool).table_provider(cmd.name.clone()).await?)
    }
}
