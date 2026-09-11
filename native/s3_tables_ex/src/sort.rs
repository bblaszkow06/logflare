use arrow_array::RecordBatch;
use arrow_ord::sort::{lexsort_to_indices, SortColumn};
use arrow_schema::{ArrowError, Schema as ArrowSchema, SortOptions};
use arrow_select::take::take_record_batch;
use iceberg::spec::{NullOrder, Schema, SortDirection, SortOrder, Transform};
use parquet::arrow::ArrowSchemaConverter;
use parquet::file::metadata::SortingColumn;

/// One field of a table's declared sort order, resolved against the arrow
/// schema the writer feeds and the parquet schema it produces.
///
/// `column_index` addresses the top-level arrow column to sort on;
/// `leaf_index` is the position of that column's parquet leaf, which is what
/// the row group's `sorting_columns` footer entry refers to and which differs
/// from `column_index` as soon as an earlier column is a map or a list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortField {
    column_index: usize,
    leaf_index: i32,
    options: SortOptions,
}

/// Resolves a table's default sort order into sortable arrow columns.
///
/// Only `Transform::Identity` is supported: rows are sorted on the column
/// values themselves, so a declared transform we cannot reproduce is an error
/// rather than a silently unsorted file. An unsorted table resolves to an
/// empty list.
pub fn resolve(
    sort_order: &SortOrder,
    schema: &Schema,
    arrow_schema: &ArrowSchema,
) -> Result<Vec<SortField>, String> {
    if sort_order.is_unsorted() {
        return Ok(Vec::new());
    }

    let parquet_schema = ArrowSchemaConverter::new()
        .convert(arrow_schema)
        .map_err(|err| format!("{err:?}"))?;

    sort_order
        .fields
        .iter()
        .map(|field| {
            if field.transform != Transform::Identity {
                return Err(format!(
                    "unsupported sort transform: {} (only identity can be applied to a batch)",
                    field.transform
                ));
            }

            let name = schema.name_by_field_id(field.source_id).ok_or_else(|| {
                format!("sort field {} is not in the table schema", field.source_id)
            })?;

            let column_index = arrow_schema
                .index_of(name)
                .map_err(|err| format!("{err:?}"))?;

            let leaf_index = parquet_schema
                .columns()
                .iter()
                .position(|leaf| leaf.path().parts().first().map(String::as_str) == Some(name))
                .ok_or_else(|| format!("sort column has no parquet leaf: {name}"))?;

            Ok(SortField {
                column_index,
                leaf_index: leaf_index as i32,
                options: SortOptions {
                    descending: field.direction == SortDirection::Descending,
                    nulls_first: field.null_order == NullOrder::First,
                },
            })
        })
        .collect()
}

/// Reorders `batch` so its rows follow `fields`. A no-op for an empty sort
/// order.
pub fn sort_batch(batch: &RecordBatch, fields: &[SortField]) -> Result<RecordBatch, ArrowError> {
    if fields.is_empty() {
        return Ok(batch.clone());
    }

    let columns = fields
        .iter()
        .map(|field| SortColumn {
            values: batch.column(field.column_index).clone(),
            options: Some(field.options),
        })
        .collect::<Vec<_>>();

    take_record_batch(batch, &lexsort_to_indices(&columns, None)?)
}

/// The parquet footer entries advertising the row order of the written files.
pub fn sorting_columns(fields: &[SortField]) -> Vec<SortingColumn> {
    fields
        .iter()
        .map(|field| SortingColumn {
            column_idx: field.leaf_index,
            descending: field.options.descending,
            nulls_first: field.options.nulls_first,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, StringArray, TimestampMicrosecondArray};
    use iceberg::arrow::schema_to_arrow_schema;
    use iceberg::spec::{MapType, NestedField, PrimitiveType, SortField as IcebergSortField, Type};

    use super::*;

    // a map column between the tenancy columns and the timestamp, so a
    // top-level column index and its parquet leaf index diverge
    fn schema() -> Schema {
        let string = Type::Primitive(PrimitiveType::String);

        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(1, "project", string.clone())),
                Arc::new(NestedField::required(2, "source_uuid", string.clone())),
                Arc::new(NestedField::optional(
                    3,
                    "log_attributes",
                    Type::Map(MapType::new(
                        Arc::new(NestedField::map_key_element(4, string.clone())),
                        Arc::new(NestedField::map_value_element(5, string, true)),
                    )),
                )),
                Arc::new(NestedField::required(
                    6,
                    "timestamp",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )),
            ])
            .build()
            .unwrap()
    }

    fn sort_order(schema: &Schema, source_ids: &[i32]) -> SortOrder {
        let mut builder = SortOrder::builder();

        for source_id in source_ids {
            builder.with_sort_field(IcebergSortField {
                source_id: *source_id,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            });
        }

        builder.build(schema).unwrap()
    }

    fn batch(rows: &[(&str, &str, i64)]) -> RecordBatch {
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema()).unwrap());

        let projects = StringArray::from_iter_values(rows.iter().map(|row| row.0));
        let sources = StringArray::from_iter_values(rows.iter().map(|row| row.1));
        let timestamps = TimestampMicrosecondArray::from_iter_values(rows.iter().map(|row| row.2))
            .with_timezone("+00:00");

        let attributes = arrow_array::new_null_array(arrow_schema.field(2).data_type(), rows.len());

        RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(projects),
                Arc::new(sources),
                attributes,
                Arc::new(timestamps),
            ],
        )
        .unwrap()
    }

    fn rows(batch: &RecordBatch) -> Vec<(String, String, i64)> {
        let projects = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sources = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let timestamps = batch
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        (0..batch.num_rows())
            .map(|row| {
                (
                    projects.value(row).to_string(),
                    sources.value(row).to_string(),
                    timestamps.value(row),
                )
            })
            .collect()
    }

    #[test]
    fn sorts_by_project_then_source_then_timestamp() {
        let schema = schema();
        let arrow_schema = schema_to_arrow_schema(&schema).unwrap();
        let fields = resolve(&sort_order(&schema, &[1, 2, 6]), &schema, &arrow_schema).unwrap();

        let batch = batch(&[
            ("proj-b", "src-1", 30),
            ("proj-a", "src-2", 10),
            ("proj-a", "src-1", 20),
            ("proj-a", "src-1", 10),
            ("proj-b", "src-1", 20),
        ]);

        assert_eq!(
            rows(&sort_batch(&batch, &fields).unwrap()),
            vec![
                ("proj-a".to_string(), "src-1".to_string(), 10),
                ("proj-a".to_string(), "src-1".to_string(), 20),
                ("proj-a".to_string(), "src-2".to_string(), 10),
                ("proj-b".to_string(), "src-1".to_string(), 20),
                ("proj-b".to_string(), "src-1".to_string(), 30),
            ]
        );
    }

    #[test]
    fn unsorted_order_leaves_rows_alone() {
        let schema = schema();
        let arrow_schema = schema_to_arrow_schema(&schema).unwrap();
        let fields = resolve(&SortOrder::unsorted_order(), &schema, &arrow_schema).unwrap();

        assert!(fields.is_empty());

        let batch = batch(&[("proj-b", "src-1", 30), ("proj-a", "src-1", 10)]);

        assert_eq!(rows(&sort_batch(&batch, &fields).unwrap()), rows(&batch));
    }

    #[test]
    fn sorting_columns_address_parquet_leaves() {
        let schema = schema();
        let arrow_schema = schema_to_arrow_schema(&schema).unwrap();
        let fields = resolve(&sort_order(&schema, &[1, 6]), &schema, &arrow_schema).unwrap();

        // the map column expands into two leaves, so `timestamp` is top-level
        // column 3 but parquet leaf 4
        assert_eq!(
            sorting_columns(&fields),
            vec![
                SortingColumn {
                    column_idx: 0,
                    descending: false,
                    nulls_first: true,
                },
                SortingColumn {
                    column_idx: 4,
                    descending: false,
                    nulls_first: true,
                },
            ]
        );
    }

    #[test]
    fn non_identity_transform_is_rejected() {
        let schema = schema();
        let arrow_schema = schema_to_arrow_schema(&schema).unwrap();

        let mut builder = SortOrder::builder();
        builder.with_sort_field(IcebergSortField {
            source_id: 6,
            transform: Transform::Day,
            direction: SortDirection::Ascending,
            null_order: NullOrder::First,
        });
        let day_order = builder.build(&schema).unwrap();

        assert!(resolve(&day_order, &schema, &arrow_schema)
            .unwrap_err()
            .contains("unsupported sort transform"));
    }
}
