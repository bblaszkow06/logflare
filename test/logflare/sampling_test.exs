defmodule Logflare.SamplingTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Logflare.LogEvent
  alias Logflare.Sampling

  defp make_events(count) do
    for _ <- 1..count, do: %LogEvent{}
  end

  describe "sample/2" do
    test "returns all events when ratio is nil" do
      events = make_events(10)
      assert Sampling.sample(events, nil) == events
    end

    test "returns all events when ratio is 1.0" do
      events = make_events(10)
      assert Sampling.sample(events, 1.0) == events
    end

    test "returns empty list when ratio is 0.0" do
      assert Sampling.sample(make_events(10), 0.0) == []
    end

    test "filters events probabilistically" do
      events = make_events(10_000)
      sampled = Sampling.sample(events, 0.5)

      # With 10k events and 0.5 ratio, expect roughly 5000 ± reasonable margin
      assert length(sampled) > 4000
      assert length(sampled) < 6000
    end

    test "returns subset of original events" do
      events = make_events(100)
      sampled = Sampling.sample(events, 0.5)

      assert Enum.all?(sampled, &(&1 in events))
    end

    test "low ratio keeps fewer events" do
      events = make_events(10_000)
      sampled = Sampling.sample(events, 0.1)

      assert length(sampled) > 500
      assert length(sampled) < 1500
    end
  end
end
