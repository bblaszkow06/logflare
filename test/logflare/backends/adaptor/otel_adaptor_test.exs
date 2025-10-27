defmodule Logflare.Backends.Adaptor.OtelAdaptorTest do
  use Logflare.DataCase, async?: true

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.OpenTelemetryAdaptor

  @subject OpenTelemetryAdaptor

  describe "config cast and validate" do
    test "enforce valid endpoint" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})
      refute changeset.valid?
      assert errors_on(changeset).endpoint == ["can't be blank"]

      invalid_endpoint = "not_an_endpoint"
      changeset = Adaptor.cast_and_validate_config(@subject, %{"endpoint" => invalid_endpoint})
      refute changeset.valid?
      assert errors_on(changeset).endpoint == ["has invalid format"]

      valid_endpoint = "http://localhost:4318"
      changeset = Adaptor.cast_and_validate_config(@subject, %{"endpoint" => valid_endpoint})
      assert changeset.valid?
    end

    test "add defaults" do
      valid_endpoint = "http://localhost:4318"

      data =
        Adaptor.cast_and_validate_config(@subject, %{"endpoint" => valid_endpoint})
        |> Ecto.Changeset.apply_changes()

      assert map_size(data) == 4
      assert data.endpoint == valid_endpoint
      assert data.gzip == true
      assert data.protocol == "http/json"
      assert data.headers == %{}
    end
  end

  describe "config transformation" do
    # TODO
  end
end
