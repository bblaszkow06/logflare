defmodule Logflare.Backends.Adaptor.OtlpAdaptor do
  @moduledoc """
  Adaptor sending logs to ingest compatible with [OTLP](https://opentelemetry.io/docs/specs/otlp/#protocol-details)
  This adaptor is **ingest-only**

  ## Configuration
  - `:endpoint` - URL of OTLP Endpoint
  - `:protocol` - Protocol used for sending logs. Currently supported is "http/protobuf", with "grpc" coming soon.
  - `:gzip` - Enables gzip compression of request
  - `:headers` - A map of additional headers to set when making an HTTP request
  """

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased

  @behaviour Logflare.Backends.Adaptor

  @doc """
  Returns a list of supported protocols
  """
  @spec protocols() :: [String.t()]
  def protocols() do
    [
      # "grpc",
      "http/protobuf"
    ]
  end

  def child_spec(init_arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [init_arg]}
    }
  end

  @impl Adaptor
  def start_link({source, backend}) do
    HttpBased.Pipeline.start_link(source, backend, __MODULE__.Client)
  end

  @impl Adaptor
  def cast_config(params) do
    defaults = %{
      gzip: true,
      protocol: "http/protobuf",
      headers: %{}
    }

    types = %{
      endpoint: :string,
      protocol: :string,
      gzip: :boolean,
      headers: {:map, :string}
    }

    {%{}, types}
    |> Ecto.Changeset.change(defaults)
    |> Ecto.Changeset.cast(params, Map.keys(types))
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:endpoint])
    |> Ecto.Changeset.validate_format(:endpoint, ~r/https?\:\/\/.+/)
    |> Ecto.Changeset.validate_inclusion(:protocol, protocols())
  end

  defmodule Client do
    alias Logflare.Backends.Adaptor.OtlpAdaptor.ProtobufFormatter
    alias Logflare.Backends.Adaptor.HttpBased

    @behaviour HttpBased.Client

    @impl HttpBased.Client
    def send_logs(config, log_events, metadata) do
      config
      |> new()
      |> Tesla.post("/v1/logs", log_events, opts: [metadata: metadata])

      :ok
    end

    @impl HttpBased.Client
    def test_connection(config) do
      config
      |> new()
      |> Tesla.post("/v1/logs", [])
      |> case do
        {:ok, %Tesla.Env{status: 200, body: %{partial_success: %{error_message: ""}}}} -> :ok
        {:ok, env} -> {:error, env}
        {:error, _reason} = err -> err
      end
    end

    defp new(config) do
      # FIXME: remove testing credentials
      config =
        Map.merge(config, %{
          endpoint: "https://otlp.last9.io:443",
          username: "swmansion",
          password: "30297c4f78bd",
          headers: %{},
          gzip: false
        })

      HttpBased.Client.new(
        url: config.endpoint,
        formatter: ProtobufFormatter,
        basic_auth: [username: config.username, password: config.password],
        gzip: config.gzip,
        json: false,
        headers: config.headers
      )
    end
  end
end
