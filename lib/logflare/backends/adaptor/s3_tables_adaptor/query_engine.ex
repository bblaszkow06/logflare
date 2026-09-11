defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.QueryEngine do
  @moduledoc """
  DuckDB query engine for the S3 Tables backend, backed by the official DuckDB
  ADBC driver.

  The ADBC driver ships libduckdb, which can load the signed
  `iceberg`/`httpfs`/`aws` extensions — NIF libs that compile DuckDB from source
  cannot. This module is a thin, stateless seam over ADBC: it builds the
  supervised child specs for a backend's in-memory DuckDB `Adbc.Database` +
  `Adbc.Connection` and runs
  statements against a connection, returning a neutral result
  map so the rest of the query path (`QuerySession`) never touches `Adbc.*`
  types.

  """

  alias Logflare.Backends
  alias Logflare.Backends.Backend

  @driver :duckdb

  @type result :: %{columns: [String.t()], rows: [[term()]]}

  @spec database_name(Backend.t()) :: GenServer.name()
  def database_name(%Backend{} = backend), do: Backends.via_backend(backend, __MODULE__.Database)

  @spec connection_name(Backend.t()) :: GenServer.name()
  def connection_name(%Backend{} = backend),
    do: Backends.via_backend(backend, __MODULE__.Connection)

  # In-memory DuckDB; the `:duckdb` driver is downloaded at compile time via
  # `config :adbc, :drivers` (see config/config.exs), not fetched at runtime.
  @spec database_child_spec(Backend.t()) :: {module(), keyword()}
  def database_child_spec(%Backend{} = backend) do
    {Adbc.Database, driver: @driver, process_options: [name: database_name(backend)]}
  end

  @spec connection_child_spec(Backend.t()) :: {module(), keyword()}
  def connection_child_spec(%Backend{} = backend) do
    {Adbc.Connection,
     database: database_name(backend), process_options: [name: connection_name(backend)]}
  end

  @spec execute(GenServer.server(), String.t(), [term()], keyword()) ::
          {:ok, result()} | {:error, term()}
  def execute(conn, sql, params, _opts) do
    case Adbc.Connection.query(conn, sql, params) do
      {:ok, result} -> {:ok, shape_result(result)}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec shape_result(Adbc.Result.t()) :: result()
  defp shape_result(%Adbc.Result{data: data} = result) do
    columns =
      data
      |> List.first([])
      |> Enum.map(& &1.field.name)

    column_map = Adbc.Result.to_map(result)

    rows =
      case Enum.map(columns, &Map.fetch!(column_map, &1)) do
        [] -> []
        column_lists -> Enum.zip_with(column_lists, &Function.identity/1)
      end

    %{columns: columns, rows: rows}
  end
end
