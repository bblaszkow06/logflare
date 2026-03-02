defmodule Logflare.Repo.Migrations.AddSamplingRatioToSourcesAndBackends do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :sampling_ratio, :float
    end

    alter table(:backends) do
      add :sampling_ratio, :float
    end
  end
end
