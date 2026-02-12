defmodule Logflare.Backends.CacheTest do
  @moduledoc false
  alias Logflare.Backends
  alias Logflare.Backends.CacheWarmer
  use Logflare.DataCase

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend,
        sources: [source]
      )

    {:ok, backend: backend, source: source, user: user}
  end

  test "warmer", %{user: user} do
    assert {:ok, []} = CacheWarmer.execute(nil)

    source =
      insert(:source,
        user: user,
        log_events_updated_at: NaiveDateTime.shift(NaiveDateTime.utc_now(), hour: -2)
      )

    backend = insert(:backend, sources: [source])

    assert {:ok, [_ | _] = pairs} = CacheWarmer.execute(nil)
    assert {:ok, true} = Cachex.put_many(Backends.Cache, pairs)

    Backends
    |> reject(:get_backend, 1)

    assert Backends.Cache.get_backend(backend.id)
  end

  test "list_backends and clear", %{source: source, backend: backend} do
    assert Cachex.size!(Backends.Cache) == 0
    backend_id = backend.id

    assert [%Backends.Backend{id: ^backend_id}] =
             Backends.Cache.list_backends(source_id: source.id)

    assert Cachex.size!(Backends.Cache) == 1
    assert :ok = Backends.Cache.clear_list_backends(source.id)
    assert Cachex.size!(Backends.Cache) == 0
  end
end
