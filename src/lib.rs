use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use clickhouse_arrow::{NativeClient, native::block::Block};
use etl::{
    destination::Destination,
    error::{ErrorKind, EtlResult},
    etl_error,
    store::{schema::SchemaStore, state::StateStore},
    types::{TableId, TableSchema},
};
use futures_util::StreamExt;
use tokio::task::JoinSet;
use tracing::{debug, info};

mod encoding;
mod schema;

#[derive(Clone)]
pub struct CHDestination<S> {
    store: S,
    client: Arc<NativeClient>,
    config: Config,
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TruncateBehavior {
    Always,
    Never,
    CdcOnly,
    LoadOnly,
}

impl TruncateBehavior {
    fn should_truncate(&self, is_cdc: bool) -> bool {
        match self {
            TruncateBehavior::Always => true,
            TruncateBehavior::Never => false,
            TruncateBehavior::CdcOnly => is_cdc,
            TruncateBehavior::LoadOnly => !is_cdc,
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Config {
    pub truncate_behavior: TruncateBehavior,
}

fn ch_error_to_etl_error(e: clickhouse_arrow::Error) -> etl::error::EtlError {
    etl_error!(
        ErrorKind::DestinationError,
        "ClickHouse error",
        format!("ClickHouse error: {}", e)
    )
}

async fn send(client: &NativeClient, table_name: &str, block: Block) -> EtlResult<()> {
    let query_id = Some(clickhouse_arrow::Qid::new());
    let query = format!("INSERT INTO {table_name} FORMAT Native");
    let result = client
        .insert(query, block, query_id)
        .await
        .map_err(ch_error_to_etl_error)?;
    tokio::pin!(result);
    while let Some(result) = result.next().await {
        result.map_err(ch_error_to_etl_error)?;
    }
    Ok(())
}

impl<S: StateStore + SchemaStore + Send + Sync> CHDestination<S> {
    pub fn new(store: S, client: Arc<NativeClient>, config: Config) -> Self {
        Self {
            store,
            client,
            config,
        }
    }

    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Arc<TableSchema>> {
        self.store.get_table_schema(table_id).await?.ok_or_else(|| {
            etl_error!(
                ErrorKind::MissingTableSchema,
                "Table schema not found",
                format!("No schema found for table {table_id}")
            )
        })
    }
    async fn get_table_mapping(
        &self,
        table_id: &TableId,
        schema: &TableSchema,
    ) -> EtlResult<String> {
        let Some(name) = self.store.get_table_mapping(table_id).await? else {
            let name = schema.name.name.clone();
            self.store
                .store_table_mapping(*table_id, name.clone())
                .await?;
            return Ok(name);
        };
        Ok(name)
    }
    async fn truncate(&self, table_id: &TableId, is_cdc: bool) -> EtlResult<()> {
        if self.config.truncate_behavior.should_truncate(is_cdc) {
            let schema = self.get_table_schema(table_id).await?;
            let table_name = self.get_table_mapping(table_id, &schema).await?;
            let query_id = clickhouse_arrow::Qid::new();
            self.client
                .execute(format!("TRUNCATE TABLE {table_name}"), Some(query_id))
                .await
                .map_err(ch_error_to_etl_error)?;
        }
        Ok(())
    }
}

impl<S: StateStore + SchemaStore + Send + Sync> Destination for CHDestination<S> {
    fn name() -> &'static str {
        "clickhouse"
    }

    async fn truncate_table(&self, table_id: etl::types::TableId) -> EtlResult<()> {
        self.truncate(&table_id, false).await
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<etl::types::TableRow>,
    ) -> EtlResult<()> {
        use clickhouse_arrow::native::values::Value;
        let schema = self.get_table_schema(&table_id).await?;
        let table_name = self.get_table_mapping(&table_id, &schema).await?;
        let schema = schema::postgres_to_ch_schema(&schema.column_schemas)?;
        let num_rows = table_rows.len();
        let num_columns = schema.len();
        let mut buffer = Vec::new();
        buffer.resize(num_columns * num_rows, Value::Null);
        for (row_idx, row) in table_rows.into_iter().enumerate() {
            encoding::apply_row(&mut buffer, num_rows, num_columns, row_idx, row, false)?;
        }
        let block = Block {
            info: Default::default(),
            rows: num_rows as u64,
            column_types: schema,
            column_data: buffer,
        };
        send(&self.client, &table_name, block).await?;
        Ok(())
    }

    async fn write_events(&self, events: Vec<etl::types::Event>) -> EtlResult<()> {
        use etl::types::Event;
        let mut event_iter = events.into_iter().peekable();
        while event_iter.peek().is_some() {
            let mut map = HashMap::new();
            while let Some(event) = event_iter.peek() {
                if matches!(event, Event::Truncate(_)) {
                    break;
                }
                let event = event_iter.next().expect("event exists");
                match event {
                    Event::Insert(e) => {
                        map.entry(e.table_id)
                            .or_insert_with(Vec::new)
                            .push((e.table_row, false));
                    }
                    Event::Update(e) => {
                        map.entry(e.table_id)
                            .or_insert_with(Vec::new)
                            .push((e.table_row, false));
                    }
                    Event::Delete(e) => {
                        let Some((_, old_table_row)) = e.old_table_row else {
                            info!("the `DELETE` event has no row, so it was skipped");
                            continue;
                        };
                        map.entry(e.table_id)
                            .or_insert_with(Vec::new)
                            .push((old_table_row, true));
                    }
                    _ => {
                        debug!("skipping unsupported event in ch");
                    }
                }
            }
            if !map.is_empty() {
                let mut join_set = JoinSet::new();
                for (table_id, rows) in map {
                    use clickhouse_arrow::native::values::Value;
                    let schema = self.get_table_schema(&table_id).await?;
                    let table_name = self.get_table_mapping(&table_id, &schema).await?;
                    let schema = schema::postgres_to_ch_schema(&schema.column_schemas)?;
                    let num_rows = rows.len();
                    let num_columns = schema.len();
                    let mut buffer = Vec::new();
                    buffer.resize(num_columns * num_rows, Value::Null);
                    for (i, (r, del)) in rows.into_iter().enumerate() {
                        encoding::apply_row(&mut buffer, num_rows, num_columns, i, r, del)?;
                    }
                    let block = Block {
                        info: Default::default(),
                        rows: num_rows as u64,
                        column_types: schema,
                        column_data: buffer,
                    };
                    let client = self.client.clone();
                    join_set.spawn(async move { send(&client, &table_name, block).await });
                }
                while let Some(r) = join_set.join_next().await {
                    r.map_err(|_| etl_error!(ErrorKind::Unknown, "failed to join future"))??;
                }
            }
            let mut truncate_table_ids = HashSet::new();

            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate_event)) = event_iter.next() {
                    for table_id in truncate_event.rel_ids {
                        truncate_table_ids.insert(TableId::new(table_id));
                    }
                }
            }
            for table_id in truncate_table_ids {
                self.truncate(&table_id, true).await?;
            }
        }
        Ok(())
    }
}
