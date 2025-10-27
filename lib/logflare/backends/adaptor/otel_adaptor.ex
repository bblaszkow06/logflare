defmodule Logflare.Backends.Adaptor.OpenTelemetryAdaptor do
  @moduledoc """
  OpenTelemetry adaptor sending logs to OTLP-compatible ingest

  This adaptor is **ingest-only**

  ## Configuration

  - `:endpoint`
  - `:protocol` 
  - `:gzip`
  - `:headers`

  ## Implementation details

  Based on `Logflare.Backends.Adaptor.WebhookAdaptor`
  """

  alias Opentelemetry.Proto.Logs.V1.LogRecord
  alias Logflare.LogEvent
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias OpenTelemetry.Proto.Logs.V1.Resource
  alias OpenTelemetry.Proto.Logs.V1.ResourceLogs

  @behaviour Logflare.Backends.Adaptor

  def child_spec(init_arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [init_arg]}
    }
  end

  @impl Adaptor
  def start_link({source, backend} = args) do
    backend = %{backend | config: transform_config(backend)}
    WebhookAdaptor.start_link({source, backend})
  end

  @impl Adaptor
  def transform_config(%_{config: config}) do
    %{
      url: endpoint_to_url(config.endpoint, config.protocol),
      headers: Map.merge(%{"content-type" => content_type(config.protocol)}, config.headers),
      http: "http2",
      gzip: config.gzip,
      format_batch: fn log_events ->
        format_log_events(log_events, config.protocol)
      end
    }
  end

  defp endpoint_to_url(endpoint, "grpc") do
    endpoint
  end

  defp endpoint_to_url(endpoint, "http/" <> _subprotocol) do
    {:ok, uri} = URI.new(endpoint)

    uri
    |> URI.append_path("v1/logs")
    |> URI.to_string()
  end

  defp content_type(protocol)
  # defp content_type("grpc"), do: "application/x-protobuf"
  defp content_type("http/protobuf"), do: "application/x-protobuf"
  defp content_type("http/json"), do: "application/json"

  @impl Adaptor
  def cast_config(params) do
    defaults = %{
      gzip: true,
      protocol: "http/json",
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
    |> Ecto.Changeset.validate_inclusion(:protocol, ["grpc", "http/protobuf", "http/json"])
  end

  # Essentially reverse processing from `Logflare.Logs.OtelLog`
  def format_log_events(events, "http/json") do
  end

  def format_log_events(events, "http/protobuf") do
    scope_logs = Enum.map(events, &format_event/1)

    %ResourceLogs{
      resource: nil,
      scope_logs: nil
    }
  end

  defp format_event(%LogEvent{} = ev) do
    lr = %LogRecord{
      time_unix_nano: 0,
      observed_time_unix_nano: 0,
      severity_number: nil,
      severity_text: nil,
      body: nil,
      attributes: %{},
      dropped_attributes_count: 0,
      flags: 0,
      trace_id: "",
      span_id: "",
      event_name: ""
    }

    scope = %{}
    resource = %{}
  end
end
