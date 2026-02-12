defmodule Logflare.Backends.Cache do
  @moduledoc false

  alias Logflare.Backends
  alias Logflare.Utils
  import Cachex.Spec

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           __MODULE__,
           [
             warmers: [
               warmer(required: false, module: Backends.CacheWarmer, name: Backends.CacheWarmer)
             ],
             hooks:
               [
                 if(stats, do: Utils.cache_stats()),
                 Utils.cache_limit(100_000)
               ]
               |> Enum.filter(& &1),
             expiration: Utils.cache_expiration_min()
           ]
         ]}
    }
  end

  def list_backends(arg), do: apply_repo_fun(__ENV__.function, [arg])

  @doc """
  Clears cached `list_backends` queries for a specific source.
  """
  @spec clear_list_backends(source_id :: integer()) :: :ok
  def clear_list_backends(source_id) when is_integer(source_id) do
    Cachex.del(__MODULE__, {:list_backends, [[source_id: source_id]]})
    :ok
  end

  def get_backend(arg), do: apply_repo_fun(__ENV__.function, [arg])
  def get_backend_by(arg), do: apply_repo_fun(__ENV__.function, [arg])

  @doc """
  Fechtches backend by token and user id with caching.
  """
  @spec fetch_backend_by_token(binary(), integer()) ::
          {:ok, Backends.Backend.t()} | {:error, :not_found}
  def fetch_backend_by_token(token, user_id) do
    case get_backend_by(token: token, user_id: user_id) do
      nil -> {:error, :not_found}
      backend -> {:ok, backend}
    end
  end

  @doc """
  Busts entries created by `fetch_backend_by_token/2`
  """
  @spec clear_backend_by_token(binary(), integer()) :: :ok
  def clear_backend_by_token(token, user_id) do
    Cachex.del(__MODULE__, {:get_backend_by, [[token: token, user_id: user_id]]})
    :ok
  end

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Backends, arg1, arg2)
  end
end
