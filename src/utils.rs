use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;

// Borrowed from datafusion-table-providers::flight::exec
pub fn enforce_schema(rb: RecordBatch, target_schema: &SchemaRef) -> arrow::error::Result<RecordBatch> {
    if target_schema.fields.is_empty() || rb.schema() == *target_schema {
        Ok(rb)
    } else if target_schema.contains(rb.schema_ref()) {
        rb.with_schema(target_schema.clone())
    } else {
        let columns = target_schema
            .fields
            .iter()
            .map(|field| {
                rb.column_by_name(field.name())
                    .ok_or(ArrowError::SchemaError(format!(
                        "Required field `{}` is missing from the flight response",
                        field.name()
                    )))
                    .and_then(|original_array| {
                        arrow_cast::cast(original_array.as_ref(), field.data_type())
                    })
            })
            .collect::<datafusion::common::Result<_, _>>()?;
        RecordBatch::try_new(target_schema.clone(), columns)
    }
}

pub fn convert_error(e: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}