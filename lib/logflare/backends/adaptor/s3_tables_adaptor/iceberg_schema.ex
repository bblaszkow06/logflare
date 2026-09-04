defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.IcebergSchema do
  @moduledoc """
  Iceberg table schemas for the S3 Tables backend, mirroring the ClickHouse OTEL
  tables (`otel_logs`, `otel_metrics`, `otel_traces`) so that ingestion supports
  ClickHouse's current OTEL format.

  ClickHouse `Nested` columns (e.g. `events.timestamp`, `exemplars.value`,
  `links.attributes`) are kept as flat, dotted column names for 1:1 parity —
  this is legal in Iceberg, but query engines that treat `.` as a struct-path
  separator require the identifier to be quoted, e.g. DuckDB needs
  `"events.timestamp"`.

  All sources of a backend share these three tables, so `project` and
  `source_uuid` carry the tenancy of every row and are required alongside `id`
  and `timestamp`. The mapper always emits both (`""` when the event body has
  no project path), so the NOT NULL contract holds for any event.

  Rows are partitioned by day and clustered by `(project, source_uuid,
  timestamp)`, so a tenant query (`WHERE project = $1 [AND source_uuid = $2]`)
  reads only the files whose parquet ranges cover that tenant.

  Each table is stamped with a `logflare.schema-version` property (see
  `table_properties/1`) — a hash of the table's field definitions *and*
  layout — so provisioning runs can detect drift against live tables.
  """

  alias Logflare.LogEvent.TypeDetection

  # keeps the iceberg-rust built-in commit retry loop well under the
  # append NIF timeout (see `Native.append_batch/3`)
  @commit_retry_total_timeout_ms "30000"

  @type field :: %{name: String.t(), type: String.t(), required: boolean()}
  @type partition_field :: %{field: String.t(), transform: String.t(), name: String.t()}
  @type sort_field :: %{field: String.t(), direction: String.t(), null_order: String.t()}
  @type layout :: %{partition: [partition_field()], sort_order: [sort_field()]}

  @layout %{
    partition: [%{field: "timestamp", transform: "day", name: "timestamp_day"}],
    sort_order: [
      %{field: "project", direction: "asc", null_order: "first"},
      %{field: "source_uuid", direction: "asc", null_order: "first"},
      %{field: "timestamp", direction: "asc", null_order: "first"}
    ]
  }

  @event_types [:log, :metric, :trace]
  @log_fields [
    %{name: "id", type: "string", required: true},
    %{name: "source_uuid", type: "string", required: true},
    %{name: "source_name", type: "string", required: false},
    %{name: "project", type: "string", required: true},
    %{name: "trace_id", type: "string", required: false},
    %{name: "span_id", type: "string", required: false},
    %{name: "trace_flags", type: "int", required: false},
    %{name: "severity_text", type: "string", required: false},
    %{name: "severity_number", type: "int", required: false},
    %{name: "service_name", type: "string", required: false},
    %{name: "event_message", type: "string", required: false},
    %{name: "scope_name", type: "string", required: false},
    %{name: "scope_version", type: "string", required: false},
    %{name: "scope_schema_url", type: "string", required: false},
    %{name: "resource_schema_url", type: "string", required: false},
    %{name: "resource_attributes", type: "map<string,string>", required: false},
    %{name: "scope_attributes", type: "map<string,string>", required: false},
    %{name: "log_attributes", type: "map<string,string>", required: false},
    %{name: "mapping_config_id", type: "string", required: false},
    %{name: "ingested_at", type: "timestamptz", required: false},
    %{name: "timestamp", type: "timestamptz", required: true}
  ]

  @metric_fields [
    %{name: "id", type: "string", required: true},
    %{name: "source_uuid", type: "string", required: true},
    %{name: "source_name", type: "string", required: false},
    %{name: "project", type: "string", required: true},
    %{name: "time_unix", type: "timestamptz", required: false},
    %{name: "start_time_unix", type: "timestamptz", required: false},
    %{name: "metric_name", type: "string", required: false},
    %{name: "metric_description", type: "string", required: false},
    %{name: "metric_unit", type: "string", required: false},
    %{name: "metric_type", type: "string", required: false},
    %{name: "service_name", type: "string", required: false},
    %{name: "event_message", type: "string", required: false},
    %{name: "scope_name", type: "string", required: false},
    %{name: "scope_version", type: "string", required: false},
    %{name: "scope_schema_url", type: "string", required: false},
    %{name: "resource_schema_url", type: "string", required: false},
    %{name: "resource_attributes", type: "map<string,string>", required: false},
    %{name: "scope_attributes", type: "map<string,string>", required: false},
    %{name: "attributes", type: "map<string,string>", required: false},
    %{name: "aggregation_temporality", type: "string", required: false},
    %{name: "is_monotonic", type: "boolean", required: false},
    %{name: "flags", type: "int", required: false},
    %{name: "value", type: "double", required: false},
    %{name: "count", type: "long", required: false},
    %{name: "sum", type: "double", required: false},
    %{name: "min", type: "double", required: false},
    %{name: "max", type: "double", required: false},
    %{name: "scale", type: "int", required: false},
    %{name: "zero_count", type: "long", required: false},
    %{name: "positive_offset", type: "int", required: false},
    %{name: "negative_offset", type: "int", required: false},
    %{name: "bucket_counts", type: "list<long>", required: false},
    %{name: "explicit_bounds", type: "list<double>", required: false},
    %{name: "positive_bucket_counts", type: "list<long>", required: false},
    %{name: "negative_bucket_counts", type: "list<long>", required: false},
    %{name: "quantile_values", type: "list<double>", required: false},
    %{name: "quantiles", type: "list<double>", required: false},
    %{name: "exemplars.filtered_attributes", type: "list<map<string,string>>", required: false},
    %{name: "exemplars.time_unix", type: "list<timestamptz>", required: false},
    %{name: "exemplars.value", type: "list<double>", required: false},
    %{name: "exemplars.span_id", type: "list<string>", required: false},
    %{name: "exemplars.trace_id", type: "list<string>", required: false},
    %{name: "mapping_config_id", type: "string", required: false},
    %{name: "ingested_at", type: "timestamptz", required: false},
    %{name: "timestamp", type: "timestamptz", required: true}
  ]

  @trace_fields [
    %{name: "id", type: "string", required: true},
    %{name: "source_uuid", type: "string", required: true},
    %{name: "source_name", type: "string", required: false},
    %{name: "project", type: "string", required: true},
    %{name: "trace_id", type: "string", required: false},
    %{name: "span_id", type: "string", required: false},
    %{name: "parent_span_id", type: "string", required: false},
    %{name: "trace_state", type: "string", required: false},
    %{name: "span_name", type: "string", required: false},
    %{name: "span_kind", type: "string", required: false},
    %{name: "service_name", type: "string", required: false},
    %{name: "event_message", type: "string", required: false},
    %{name: "duration", type: "long", required: false},
    %{name: "status_code", type: "string", required: false},
    %{name: "status_message", type: "string", required: false},
    %{name: "scope_name", type: "string", required: false},
    %{name: "scope_version", type: "string", required: false},
    %{name: "resource_attributes", type: "map<string,string>", required: false},
    %{name: "span_attributes", type: "map<string,string>", required: false},
    %{name: "events.timestamp", type: "list<timestamptz>", required: false},
    %{name: "events.name", type: "list<string>", required: false},
    %{name: "events.attributes", type: "list<map<string,string>>", required: false},
    %{name: "links.trace_id", type: "list<string>", required: false},
    %{name: "links.span_id", type: "list<string>", required: false},
    %{name: "links.trace_state", type: "list<string>", required: false},
    %{name: "links.attributes", type: "list<map<string,string>>", required: false},
    %{name: "mapping_config_id", type: "string", required: false},
    %{name: "ingested_at", type: "timestamptz", required: false},
    %{name: "timestamp", type: "timestamptz", required: true}
  ]

  @doc """
  Returns all event types with a corresponding Iceberg table.
  """
  @spec event_types() :: [TypeDetection.event_type()]
  def event_types, do: @event_types

  @doc """
  Returns the Iceberg table name for a given event type.
  """
  @spec table_name(TypeDetection.event_type()) :: String.t()
  def table_name(:log), do: "otel_logs"
  def table_name(:metric), do: "otel_metrics"
  def table_name(:trace), do: "otel_traces"

  @doc """
  Returns the ordered column definitions for a given event type's Iceberg table.
  """
  @spec fields(TypeDetection.event_type()) :: [field()]
  def fields(:log), do: @log_fields
  def fields(:metric), do: @metric_fields
  def fields(:trace), do: @trace_fields

  @doc """
  Returns the physical layout — partition spec and sort order — applied to a
  given event type's table at creation.
  """
  @spec layout(TypeDetection.event_type()) :: layout()
  def layout(event_type) when event_type in @event_types, do: @layout

  # Calculate schema version at compile time, raising if the layout references
  # a column the table does not have
  defmacrop schema_version_m(event_type) when is_atom(event_type) do
    fields =
      case event_type do
        :log -> @log_fields
        :metric -> @metric_fields
        :trace -> @trace_fields
      end

    field_names = Enum.map(fields, & &1.name)

    for %{field: name} <- @layout.partition ++ @layout.sort_order, name not in field_names do
      raise "#{event_type} table layout references unknown column #{inspect(name)}"
    end

    field_lines =
      Enum.map(fields, fn %{name: name, type: type, required: required} ->
        "#{name}:#{type}:#{required}"
      end)

    partition_lines =
      Enum.map(@layout.partition, fn %{field: field, transform: transform, name: name} ->
        "partition:#{field}:#{transform}:#{name}"
      end)

    sort_lines =
      Enum.map(@layout.sort_order, fn %{field: field, direction: dir, null_order: null_order} ->
        "sort:#{field}:#{dir}:#{null_order}"
      end)

    version =
      :sha256
      |> :crypto.hash(Enum.join(field_lines ++ partition_lines ++ sort_lines, "\n"))
      |> Base.encode16(case: :lower)

    quote do
      unquote(version)
    end
  end

  @doc """
  Returns the schema version for a given event type's table: a SHA-256 hash
  of the canonical field definitions and layout.
  """
  @spec schema_version(TypeDetection.event_type()) :: String.t()
  def schema_version(:log), do: schema_version_m(:log)
  def schema_version(:metric), do: schema_version_m(:metric)
  def schema_version(:trace), do: schema_version_m(:trace)

  @doc """
  Returns the Iceberg table properties stamped on a table at creation.
  """
  @spec table_properties(TypeDetection.event_type()) :: %{String.t() => String.t()}
  def table_properties(event_type) do
    %{
      "logflare.schema-version" => schema_version(event_type),
      "commit.retry.total-timeout-ms" => @commit_retry_total_timeout_ms
    }
  end
end
