defmodule Logflare.Backends.Adaptor.HttpBased.Client do
  defguardp is_possible_pool(value)
            when not is_nil(value) and not is_boolean(value) and is_atom(value)

  def adapter_config(http_type \\ "http", pool_name \\ nil) do
    cond do
      is_possible_pool(pool_name) ->
        {Tesla.Adapter.Finch, name: pool_name, receive_timeout: 5_000}

      http_type == "http2" ->
        {Tesla.Adapter.Finch, name: Logflare.FinchDefault, receive_timeout: 5_000}

      true ->
        {Tesla.Adapter.Finch, name: Logflare.FinchDefaultHttp1, receive_timeout: 5_000}
    end
  end

  @callback client(config :: map()) :: Tesla.Client.t()

  defmodule LogEventTransformer do
    alias Logflare.LogEvent
    @behaviour Tesla.Middleware

    @impl true
    def call(env, next, _options) do
      case env.body do
        [_ | _] -> for %LogEvent{body: body} <- env.body, do: body
        _ -> Tesla.run(env, next)
      end
    end
  end
end
