defmodule Logflare.Backends.Adaptor.ElasticAdaptor do
  @moduledoc """

  Ingestion uses Filebeat HTTP input.

  https://www.elastic.co/guide/en/beats/filebeat/current/filebeat-input-http_endpoint.html

  Basic auth implementation reference:
  https://datatracker.ietf.org/doc/html/rfc7617

  """

  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Utils

  @behaviour Logflare.Backends.Adaptor

  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    backend = %{backend | config: transform_config(backend)}
    WebhookAdaptor.start_link({source, backend})
  end

  @impl Logflare.Backends.Adaptor
  def transform_config(%_{config: config}) do
    basic_auth = Utils.encode_basic_auth(config)

    %{
      url: config.url,
      http: "http1",
      headers:
        if basic_auth do
          %{"Authorization" => "Basic #{basic_auth}"}
        else
          %{}
        end
        |> Map.put("Content-Type", "application/json")
    }
  end

  @impl Logflare.Backends.Adaptor
  def redact_config(config) do
    Map.replace_lazy(config, :password, fn _ -> "REDACTED" end)
  end

  @impl Logflare.Backends.Adaptor
  @spec test_connection(Backend.t()) :: :ok | {:error, term()}
  def test_connection(%Backend{} = backend) do
    backend = %{backend | config: transform_config(backend)}

    WebhookAdaptor.test_connection(backend, "{[]}")
    |> case do
      {:error, ~s|Unexpected response: 400 %{"message" => "malformed JSON object| <> _} -> :ok
      other_error -> other_error
    end
  end

  @impl Logflare.Backends.Adaptor
  def cast_config(params, existing_config \\ %{}) do
    {existing_config, %{url: :string, username: :string, password: :string}}
    |> Ecto.Changeset.cast(params, [:username, :password, :url])
    |> validate_user_pass()
  end

  defp validate_user_pass(changeset) do
    user = Ecto.Changeset.get_field(changeset, :username)
    pass = Ecto.Changeset.get_field(changeset, :password)
    user_pass = [user, pass]

    if user_pass != [nil, nil] and Enum.any?(user_pass, &is_nil/1) do
      msg = "Both username and password must be provided for basic auth"

      changeset
      |> Ecto.Changeset.add_error(:username, msg)
      |> Ecto.Changeset.add_error(:password, msg)
    else
      changeset
    end
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    import Ecto.Changeset

    changeset
    |> validate_required([:url])
  end
end
