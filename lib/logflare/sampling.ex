defmodule Logflare.Sampling do
  @moduledoc false

  alias Logflare.LogEvent

  @spec sample([LogEvent.t()], float() | nil) :: [LogEvent.t()]
  def sample(events, nil), do: events
  def sample(events, ratio) when ratio == 1.0, do: events
  def sample(_events, ratio) when ratio == 0.0, do: []

  def sample(events, ratio) when is_float(ratio) and ratio > 0.0 and ratio < 1.0 do
    Enum.filter(events, fn _event -> :rand.uniform() < ratio end)
  end
end
