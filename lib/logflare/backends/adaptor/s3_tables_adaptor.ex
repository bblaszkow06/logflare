defmodule Logflare.Backends.Adaptor.S3TablesAdaptor do
  @moduledoc """
  Backend adaptor that writes batches of logs to AWS S3 Tables (Apache Iceberg).

  Runs consolidated: one adaptor tree per backend across all sources (see
  `Logflare.Backends.ConsolidatedSup`).
  """

  use Supervisor

  import Logflare.Utils.Guards

  alias __MODULE__.CatalogManager
  alias __MODULE__.Native
  alias __MODULE__.Pipeline
  alias __MODULE__.QuerySession
  alias Ecto.Changeset
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.Backend
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.QueryError
  alias Logflare.Sql

  @behaviour Adaptor

  @min_batch_timeout 5_000
  @max_batch_timeout 60_000

  @doc false
  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Adaptor
  def consolidated_ingest?, do: true

  @doc false
  @impl Adaptor
  @spec start_link(Backend.t()) :: Supervisor.on_start()
  def start_link(%Backend{} = backend) do
    Supervisor.start_link(__MODULE__, backend, name: Backends.via_backend(backend, __MODULE__))
  end

  @doc false
  @impl Adaptor
  def cast_config(%{} = params, existing_config \\ %{}) do
    types = %{
      table_bucket_arn: :string,
      namespace: :string,
      access_key_id: :string,
      secret_access_key: :string,
      batch_timeout: :integer
    }

    {existing_config, types}
    |> Changeset.cast(params, Map.keys(types))
  end

  @doc false
  @impl Adaptor
  def validate_config(%Changeset{} = changeset) do
    import Ecto.Changeset

    changeset
    |> validate_required([:table_bucket_arn, :namespace, :access_key_id, :secret_access_key])
    |> validate_number(:batch_timeout,
      greater_than_or_equal_to: @min_batch_timeout,
      less_than_or_equal_to: @max_batch_timeout
    )
  end

  @impl Adaptor
  def redact_config(config) do
    if config.secret_access_key do
      Map.put(config, :secret_access_key, "REDACTED")
    else
      config
    end
  end

  @doc """
  Probes connectivity and credentials by constructing an S3 Tables catalog handle.
  """
  @impl Adaptor
  @spec test_connection(Backend.t()) :: :ok | {:error, term()}
  def test_connection(%Backend{} = backend) do
    config = Adaptor.get_backend_config(backend)

    case Native.init_catalog(config) do
      {:ok, _catalog} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl Adaptor
  def supports_default_ingest?, do: true

  @doc """
  Queries the backend's live DuckDB session (see `QuerySession`).

  Accepts a bare SQL string, `{sql, params}`, or the endpoint-provided
  `{sql, declared_params, input_params[, endpoint_query]}` shapes.
  """
  @impl Adaptor
  def execute_query(%Backend{} = backend, query_string, opts)
      when is_non_empty_binary(query_string) and is_list(opts) do
    execute_query(backend, {query_string, []}, opts)
  end

  def execute_query(%Backend{} = backend, {query_string, params}, opts)
      when is_non_empty_binary(query_string) and is_list(params) do
    run_query(backend, query_string, params, opts)
  end

  def execute_query(%Backend{} = backend, {query_string, declared_params, input_params}, opts)
      when is_non_empty_binary(query_string) and is_list(declared_params) and is_map(input_params) do
    run_query(backend, query_string, order_params(declared_params, input_params), opts)
  end

  def execute_query(
        %Backend{} = backend,
        {query_string, declared_params, input_params, _endpoint_query},
        opts
      )
      when is_non_empty_binary(query_string) and is_list(declared_params) and is_map(input_params) do
    run_query(backend, query_string, order_params(declared_params, input_params), opts)
  end

  @doc """
  Orders endpoint parameter values to match the `$1..$n` placeholders in the
  transformed query (see `Logflare.Sql.map_query_values/2`).
  """
  @impl Adaptor
  def map_query_parameters(original_query, _transformed_query, _declared_params, input_params) do
    Sql.map_query_values(original_query, input_params)
  end

  @doc """
  Prepares an already-transformed query for the DuckDB session, rewriting
  `@param` references into the `$1..$n` placeholders DuckDB binds
  (see `map_query_parameters/4` for the matching value order).

  `:duckdb_sql` queries are emitted by `Logflare.Sql.DialectTransformer.DuckDb`
  and need no table rewriting.
  """
  @impl Adaptor
  def transform_query(query, :duckdb_sql, _context) when is_non_empty_binary(query) do
    {:ok, Sql.to_positional_parameters(query, dialect: "duckdb")}
  end

  def transform_query(_query, from_language, _context) do
    {:error, "Transformation from #{from_language} to S3 Tables (DuckDB) not supported"}
  end

  # Fallback ordering for direct callers; endpoints use `map_query_parameters/4` above.
  @spec order_params([String.t()], map()) :: [term()]
  defp order_params(declared_params, input_params) do
    Enum.map(declared_params, &Map.get(input_params, &1))
  end

  @spec run_query(Backend.t(), String.t(), [term()], keyword()) ::
          {:ok, QueryResult.t()} | {:error, QueryError.t()}
  defp run_query(backend, sql, params, opts) do
    case QuerySession.execute(backend, sql, params, opts) do
      {:ok, _result} = ok -> ok
      # TODO(step3): surface a redacted DuckDB message (never echo raw bootstrap SQL, which carries the secret).
      {:error, reason} -> {:error, to_query_error(reason)}
    end
  end

  @spec to_query_error(term()) :: QueryError.t()
  defp to_query_error(%Adbc.Error{} = error) do
    %QueryError{kind: adbc_error_kind(error), raw_error: error, backend: __MODULE__}
  end

  defp to_query_error(reason) do
    %QueryError{kind: :backend_error, raw_error: reason, backend: __MODULE__}
  end

  @spec adbc_error_kind(Adbc.Error.t()) :: QueryError.kind()
  defp adbc_error_kind(%Adbc.Error{message: message}) when is_binary(message) do
    if message =~ ~r/Parser Error|Binder Error|Catalog Error|Syntax [Ee]rror/ do
      :invalid_query
    else
      :backend_error
    end
  end

  defp adbc_error_kind(%Adbc.Error{}), do: :backend_error

  @doc false
  @impl Supervisor
  def init(%Backend{} = backend) do
    # create the startup queue and its generation, before any producer/traffic exists
    IngestEventQueue.upsert_tid({:consolidated, backend.id, nil})
    IngestEventQueue.current_generation_tid({:consolidated, backend.id})

    config = Adaptor.get_backend_config(backend)

    pipeline_args = [
      name: Backends.via_backend(backend, Pipeline),
      backend: backend,
      batch_timeout: config.batch_timeout
    ]

    children =
      if(Application.get_env(:logflare, :env) != :test,
        do: [CatalogManager.child_spec(backend)],
        else: []
      ) ++
        [
          Pipeline.child_spec(pipeline_args)
        ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
