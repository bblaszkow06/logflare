defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.QuerySession do
  @moduledoc """
  Per-backend DuckDB query router for the S3 Tables backend.

  Runs as a child of `QueryBackendSup` (alongside the backend's supervised
  `Adbc.Database` and `Adbc.Connection`), registered via
  `Logflare.Backends.via_backend/2`. On the first query it runs the bootstrap
  statements once against the backend's supervised connection — install/load the
  iceberg extensions, create the S3 credential secret, `ATTACH` the S3 Tables
  catalog, and `USE` its namespace — then reuses that connection for subsequent
  queries. Bootstrap is never run in `init/1`, so unqueried backends hold no
  DuckDB state.

  Secret-bearing bootstrap statements are never logged.
  """

  use GenServer

  import Logflare.Utils.Guards

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.QueryEngine
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.QuerySup
  alias Logflare.Backends.Backend

  # TODO(step3): make the per-query timeout configurable and flow it to GenServer.call + engine.
  @query_timeout :timer.seconds(60)
  # TODO(step3): expose memory_limit (and maybe threads) via validated backend config.
  @memory_limit "2GB"
  @catalog_alias "s3t"
  @secret_name "logflare_s3t"

  @type state :: %{backend: Backend.t(), connection: GenServer.name() | nil}

  @doc false
  @spec child_spec(Backend.t()) :: Supervisor.child_spec()
  def child_spec(%Backend{} = backend) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [backend]},
      restart: :transient
    }
  end

  @doc false
  @spec start_link(Backend.t()) :: GenServer.on_start()
  def start_link(%Backend{} = backend) do
    GenServer.start_link(__MODULE__, backend, name: Backends.via_backend(backend, __MODULE__))
  end

  @doc """
  Executes `sql` (with positional `$1..$n` `params`) against the backend's live
  DuckDB connection, starting and bootstrapping the subtree on first use.
  """
  @spec execute(Backend.t(), String.t(), [term()], keyword()) ::
          {:ok, QueryResult.t()} | {:error, term()}
  def execute(%Backend{} = backend, sql, params, _opts \\ [])
      when is_non_empty_binary(sql) and is_list(params) do
    with {:ok, _pid} <- QuerySup.ensure_session(backend) do
      backend
      |> Backends.via_backend(__MODULE__)
      |> GenServer.call({:execute, sql, params}, @query_timeout)
    end
  end

  @doc """
  Builds the ordered DuckDB session bootstrap statements for a backend config.

  Pure and side-effect free. **Contains secret values — never log the output.**
  """
  @spec bootstrap_statements(map()) :: [String.t()]
  def bootstrap_statements(%{} = config) do
    region = region_from_arn(config.table_bucket_arn)

    [
      "INSTALL aws",
      "INSTALL httpfs",
      "INSTALL iceberg",
      "LOAD aws",
      "LOAD httpfs",
      "LOAD iceberg",
      create_secret_statement(config, region),
      attach_statement(config),
      "USE #{@catalog_alias}.#{quote_identifier(config.namespace)}",
      "SET memory_limit = '#{@memory_limit}'"
    ]
  end

  @doc """
  Extracts the AWS region from an S3 Tables bucket ARN
  (`arn:aws:s3tables:<region>:<account>:bucket/<name>`).
  """
  @spec region_from_arn(String.t()) :: String.t()
  def region_from_arn(arn) when is_non_empty_binary(arn) do
    case String.split(arn, ":") do
      ["arn", _partition, _service, region | _rest] when region != "" -> region
      _ -> raise ArgumentError, "could not parse region from S3 Tables ARN"
    end
  end

  @impl GenServer
  def init(%Backend{} = backend) do
    {:ok, %{backend: backend, connection: nil}}
  end

  @impl GenServer
  def handle_call({:execute, sql, params}, _from, state) do
    case ensure_ready(state) do
      {:ok, %{connection: conn} = state} ->
        # TODO(step3): detect dead-session errors, recycle the connection instead of surfacing raw.
        case QueryEngine.execute(conn, sql, params, []) do
          {:ok, result} -> {:reply, {:ok, to_query_result(result)}, state}
          {:error, _reason} = error -> {:reply, error, state}
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @spec ensure_ready(state()) :: {:ok, state()} | {:error, term()}
  defp ensure_ready(%{connection: conn} = state) when not is_nil(conn), do: {:ok, state}

  defp ensure_ready(%{connection: nil, backend: backend} = state) do
    config = Adaptor.get_backend_config(backend)
    conn = QueryEngine.connection_name(backend)

    with :ok <- run_bootstrap(conn, config) do
      {:ok, %{state | connection: conn}}
    end
  end

  @spec run_bootstrap(GenServer.name(), map()) :: :ok | {:error, term()}
  defp run_bootstrap(conn, config) do
    config
    |> bootstrap_statements()
    |> Enum.reduce_while(:ok, fn statement, :ok ->
      case QueryEngine.execute(conn, statement, [], []) do
        {:ok, _result} -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  @spec to_query_result(QueryEngine.result()) :: QueryResult.t()
  defp to_query_result(%{columns: columns, rows: rows}) do
    rows
    |> Enum.map(fn row -> columns |> Enum.zip(row) |> Map.new() end)
    |> QueryResult.new()
  end

  @spec create_secret_statement(map(), String.t()) :: String.t()
  defp create_secret_statement(config, region) do
    "CREATE OR REPLACE SECRET #{@secret_name} (" <>
      "TYPE s3, " <>
      "KEY_ID '#{escape_string(config.access_key_id)}', " <>
      "SECRET '#{escape_string(config.secret_access_key)}', " <>
      "REGION '#{escape_string(region)}')"
  end

  @spec attach_statement(map()) :: String.t()
  defp attach_statement(config) do
    # IF NOT EXISTS keeps bootstrap idempotent so a retry after a mid-bootstrap
    # failure does not error on an already-attached catalog.
    "ATTACH IF NOT EXISTS '#{escape_string(config.table_bucket_arn)}' AS #{@catalog_alias} " <>
      "(TYPE iceberg, ENDPOINT_TYPE s3_tables)"
  end

  @spec escape_string(String.t()) :: String.t()
  defp escape_string(value), do: String.replace(value, "'", "''")

  @spec quote_identifier(String.t()) :: String.t()
  defp quote_identifier(identifier), do: ~s("#{String.replace(identifier, ~s("), ~s(""))}")
end
