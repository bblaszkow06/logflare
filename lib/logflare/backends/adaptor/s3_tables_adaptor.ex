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
  alias Logflare.Backends.Backend
  alias Logflare.Backends.IngestEventQueue

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
    QuerySession.execute(backend, query_string, params, opts)
  end

  def execute_query(%Backend{} = backend, {query_string, declared_params, input_params}, opts)
      when is_non_empty_binary(query_string) and is_list(declared_params) and is_map(input_params) do
    QuerySession.execute(backend, query_string, order_params(declared_params, input_params), opts)
  end

  def execute_query(
        %Backend{} = backend,
        {query_string, declared_params, input_params, _endpoint_query},
        opts
      )
      when is_non_empty_binary(query_string) and is_list(declared_params) and is_map(input_params) do
    QuerySession.execute(backend, query_string, order_params(declared_params, input_params), opts)
  end

  # TODO(step2): replace positional extraction with proper `@param -> $n` mapping
  # via `map_query_parameters/4` + `transform_query/3` (reversing the pg_sql rewrite).
  @spec order_params([String.t()], map()) :: [term()]
  defp order_params(declared_params, input_params) do
    Enum.map(declared_params, &Map.get(input_params, &1))
  end

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
