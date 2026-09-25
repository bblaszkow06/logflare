defmodule Logflare.Sql.DialectTransformer.DuckDb do
  @moduledoc """
  DuckDB-specific SQL transformations for the S3 Tables (Apache Iceberg) backend.

  S3 Tables backends ingest every source of a backend into the same consolidated
  Iceberg tables (`otel_logs`, `otel_metrics`, `otel_traces`), tagging each row
  with `source_uuid`. Source references are therefore rewritten into a filtered
  subquery over the consolidated table rather than renamed to a physical table.
  """

  @behaviour Logflare.Sql.DialectTransformer

  alias Logflare.Sources.Source
  alias Logflare.Sql.AstUtils
  alias Logflare.Sql.Parser
  alias Logflare.User

  # Mirrors `S3TablesAdaptor.IcebergSchema.table_name/1`. Referencing them
  # directly queries every source of the backend.
  @reserved_table_names ~w(otel_logs otel_metrics otel_traces)

  # TODO(o11y-2113): metrics/traces are only reachable by their reserved table
  # name; a source reference always resolves to the logs table.
  @source_table "otel_logs"

  @impl true
  def quote_style, do: "\""

  @impl true
  def dialect, do: "duckdb"

  @impl true
  def transform_source_name(source_name, _data), do: source_name

  @impl true
  def transform_source_relation(%{"Table" => %{"alias" => table_alias}}, source_name, data) do
    %Source{token: token} = Map.fetch!(data.source_mapping, source_name)

    build_derived_relation(
      Ecto.UUID.cast!(to_string(token)),
      table_alias || build_alias(source_name)
    )
  end

  @doc """
  Returns the consolidated Iceberg table names that may be referenced directly,
  in addition to the user's source names.
  """
  @spec reserved_table_names() :: [String.t()]
  def reserved_table_names, do: @reserved_table_names

  @doc """
  Builds transformation data for DuckDB from a user and base data.

  DuckDB requires no project/dataset metadata, so the base data is passed through.
  """
  @spec build_transformation_data(User.t(), map()) :: map()
  def build_transformation_data(%User{}, base_data), do: base_data

  @spec build_derived_relation(String.t(), map()) :: map()
  defp build_derived_relation(source_uuid, table_alias) do
    # Extract subquery AST from minimal parseable query to be put in the original query
    {:ok, [%{"Query" => %{"body" => %{"Select" => %{"from" => [%{"relation" => relation}]}}}}]} =
      Parser.parse(
        dialect(),
        "SELECT 1 FROM (SELECT * FROM #{@source_table} WHERE source_uuid = '#{source_uuid}') AS t"
      )

    # Swap the placeholder (t) alias afterwards: `table_alias` is an AST node, not renderable SQL text
    put_in(relation, ["Derived", "alias"], table_alias)
  end

  @spec build_alias(String.t()) :: map()
  defp build_alias(source_name) do
    %{"name" => AstUtils.build_identifier(source_name, quote_style()), "columns" => []}
  end
end
