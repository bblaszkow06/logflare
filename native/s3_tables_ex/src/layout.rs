use std::collections::HashMap;

use iceberg::spec::{
    NullOrder, Schema, SortDirection, SortField, SortOrder, Transform, UnboundPartitionSpec,
};
use rustler::NifMap;

/// Mirrors `Logflare.Backends.Adaptor.S3TablesAdaptor.IcebergSchema.layout()` on
/// the Elixir side: the physical layout of a table, kept out of Rust so the NIF
/// stays column-agnostic.
#[derive(Debug, NifMap)]
pub struct LayoutSpec {
    pub partition: Vec<PartitionFieldSpec>,
    pub sort_order: Vec<SortFieldSpec>,
}

#[derive(Debug, NifMap)]
pub struct PartitionFieldSpec {
    pub field: String,
    pub transform: String,
    pub name: String,
}

#[derive(Debug, NifMap)]
pub struct SortFieldSpec {
    pub field: String,
    pub direction: String,
    pub null_order: String,
}

impl LayoutSpec {
    pub fn partition_spec(
        &self,
        field_ids: &HashMap<String, i32>,
    ) -> Result<UnboundPartitionSpec, String> {
        let mut builder = UnboundPartitionSpec::builder();

        for field in &self.partition {
            builder = builder
                .add_partition_field(
                    field_id(field_ids, &field.field)?,
                    &field.name,
                    parse_transform(&field.transform)?,
                )
                .map_err(|err| format!("{err:?}"))?;
        }

        Ok(builder.build())
    }

    /// Sort fields always use the identity transform: rows are sorted on the
    /// column values themselves, so a reader's range pruning matches the
    /// predicates the query side writes.
    pub fn sort_order(
        &self,
        field_ids: &HashMap<String, i32>,
        schema: &Schema,
    ) -> Result<SortOrder, String> {
        let mut builder = SortOrder::builder();

        for field in &self.sort_order {
            builder.with_sort_field(SortField {
                source_id: field_id(field_ids, &field.field)?,
                transform: Transform::Identity,
                direction: parse_direction(&field.direction)?,
                null_order: parse_null_order(&field.null_order)?,
            });
        }

        builder.build(schema).map_err(|err| format!("{err:?}"))
    }
}

fn field_id(field_ids: &HashMap<String, i32>, name: &str) -> Result<i32, String> {
    field_ids
        .get(name)
        .copied()
        .ok_or_else(|| format!("table layout references unknown column: {name}"))
}

fn parse_transform(dsl: &str) -> Result<Transform, String> {
    match dsl {
        "identity" => Ok(Transform::Identity),
        "hour" => Ok(Transform::Hour),
        "day" => Ok(Transform::Day),
        "month" => Ok(Transform::Month),
        "year" => Ok(Transform::Year),
        other => Err(format!("unknown partition transform: {other}")),
    }
}

fn parse_direction(dsl: &str) -> Result<SortDirection, String> {
    match dsl {
        "asc" => Ok(SortDirection::Ascending),
        "desc" => Ok(SortDirection::Descending),
        other => Err(format!("unknown sort direction: {other}")),
    }
}

fn parse_null_order(dsl: &str) -> Result<NullOrder, String> {
    match dsl {
        "first" => Ok(NullOrder::First),
        "last" => Ok(NullOrder::Last),
        other => Err(format!("unknown sort null order: {other}")),
    }
}
