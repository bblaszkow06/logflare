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
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Sql
  alias Logflare.Sql.AstUtils
  alias Logflare.Sql.Parser

  @behaviour Adaptor

  @min_batch_timeout 5_000
  @max_batch_timeout 60_000

  # PoC shim: the `:pg_sql` endpoint path only accepts sources named after the
  # consolidated OTEL Iceberg tables. Superseded by the `:duckdb_sql` language
  # + `DialectTransformer.DuckDb` in a later step.
  @otel_source_names ~w(otel_logs otel_metrics otel_traces)

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
  transformed query (see `Logflare.Sql.parameter_values/2`).
  """
  @impl Adaptor
  def map_query_parameters(original_query, _transformed_query, _declared_params, input_params) do
    Sql.map_query_values(original_query, input_params)
  end

  @doc """
  Reverses the mandatory `:pg_sql` source-name rewrite so DuckDB sees the Iceberg
  table names.

  `Sql.transform(:pg_sql, ...)` rewrites each source reference to its physical
  `log_events_<token>` name; this maps those back to the source's name, which for
  the PoC must be one of the consolidated OTEL tables (see `@otel_source_names`).
  """
  @impl Adaptor
  def transform_query(query, :pg_sql, _context) when is_non_empty_binary(query) do
    with {:ok, ast} <- Parser.parse("postgres", query),
         {:ok, mapping} <- build_source_name_mapping(ast) do
      ast
      |> AstUtils.transform_recursive(mapping, &restore_source_name/2)
      |> Parser.to_string()
    end
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

  @spec build_source_name_mapping([map()]) ::
          {:ok, %{String.t() => String.t()}} | {:error, String.t()}
  defp build_source_name_mapping(ast) do
    ast
    |> collect_physical_table_names()
    |> Enum.reduce_while({:ok, %{}}, fn physical_name, {:ok, acc} ->
      case resolve_source_name(physical_name) do
        {:ok, source_name} -> {:cont, {:ok, Map.put(acc, physical_name, source_name)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  @spec collect_physical_table_names([map()]) :: [String.t()]
  defp collect_physical_table_names(ast) do
    ast
    |> AstUtils.collect_from_ast(fn
      {"Table", %{"name" => parts}} when is_list(parts) -> {:collect, parts}
      _ -> :skip
    end)
    |> List.flatten()
    |> Enum.map(& &1["value"])
    |> Enum.filter(&physical_table_name?/1)
    |> Enum.uniq()
  end

  @spec resolve_source_name(String.t()) :: {:ok, String.t()} | {:error, String.t()}
  defp resolve_source_name(physical_name) do
    token =
      physical_name
      |> String.replace_prefix("log_events_", "")
      |> String.replace("_", "-")

    # TODO(step2): cache this token -> source lookup (currently a DB hit per referenced source).
    case Sources.get_source_by_token(token) do
      %Source{name: name} when name in @otel_source_names ->
        {:ok, name}

      %Source{name: name} ->
        {:error, "source #{inspect(name)} is not a queryable OTEL table"}

      nil ->
        {:error, "no source found for #{physical_name}"}
    end
  end

  @spec physical_table_name?(term()) :: boolean()
  defp physical_table_name?(value) when is_binary(value),
    do: String.starts_with?(value, "log_events_")

  defp physical_table_name?(_value), do: false

  @spec restore_source_name({String.t(), map()}, map()) :: {:recurse, term()}
  defp restore_source_name({"Table" = key, %{"name" => parts} = table}, mapping)
       when is_list(parts) do
    restored =
      Enum.map(parts, fn part ->
        case Map.fetch(mapping, part["value"]) do
          {:ok, source_name} -> %{part | "value" => source_name}
          :error -> part
        end
      end)

    {:recurse, {key, %{table | "name" => restored}}}
  end

  defp restore_source_name(node, _mapping), do: {:recurse, node}

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
