defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.QuerySup do
  @moduledoc """
  Always-running `DynamicSupervisor` for S3 Tables query subtrees.

  Empty at boot; lazily starts `QueryBackendSup` for a backend on its first query.
  This lives outside the adaptor's ingest tree (`Logflare.Backends.ConsolidatedSup`)
  so read queries work for backends that have no sources/rules attached
  and therefore no running ingest pipeline.
  """

  use DynamicSupervisor

  alias Logflare.Backends.Adaptor.S3TablesAdaptor.QueryBackendSup
  alias Logflare.Backends.Backend

  @spec start_link(term()) :: Supervisor.on_start()
  def start_link(args) do
    DynamicSupervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  @doc """
  Ensures the query subtree for `backend` is running, starting it on demand.
  """
  @spec ensure_session(Backend.t()) :: {:ok, pid()} | {:error, term()}
  def ensure_session(%Backend{} = backend) do
    case DynamicSupervisor.start_child(__MODULE__, QueryBackendSup.child_spec(backend)) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, _reason} = error -> error
    end
  end

  @impl DynamicSupervisor
  def init(_args) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
