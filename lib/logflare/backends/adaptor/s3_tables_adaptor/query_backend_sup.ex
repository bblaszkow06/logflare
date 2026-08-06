defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.QueryBackendSup do
  @moduledoc """
  Per-backend supervision subtree for the S3 Tables DuckDB query path.

  Children (`:rest_for_one`, so a dead `Adbc.Database` recycles the connection
  and session that depend on it, and a dead `Adbc.Connection` recycles the
  session — forcing a re-bootstrap of its connection-scoped state):

    1. `Adbc.Database` — one in-memory DuckDB instance per backend, bootstrapped
       once (see `QuerySession`), holding this backend's extensions, `SECRET`,
       and `ATTACH`ed catalog.
    2. `Adbc.Connection` — this backend's single connection over the `Database`
       (a per-backend pool lands later — O11Y-2211).
    3. `QuerySession` — routes queries and runs the one-time bootstrap.
  """

  use Supervisor

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.QueryEngine
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.QuerySession
  alias Logflare.Backends.Backend

  @doc false
  @spec child_spec(Backend.t()) :: Supervisor.child_spec()
  def child_spec(%Backend{} = backend) do
    %{
      id: {__MODULE__, backend.id},
      start: {__MODULE__, :start_link, [backend]},
      restart: :temporary,
      type: :supervisor
    }
  end

  @doc false
  @spec start_link(Backend.t()) :: Supervisor.on_start()
  def start_link(%Backend{} = backend) do
    Supervisor.start_link(__MODULE__, backend, name: Backends.via_backend(backend, __MODULE__))
  end

  @impl Supervisor
  def init(%Backend{} = backend) do
    children = [
      QueryEngine.database_child_spec(backend),
      QueryEngine.connection_child_spec(backend),
      {QuerySession, backend}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
