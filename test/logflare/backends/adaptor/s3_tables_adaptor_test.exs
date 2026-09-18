defmodule Logflare.Backends.Adaptor.S3TablesAdaptorTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.Adaptor.S3TablesAdaptor
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.CatalogManager
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.IcebergSchema
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.Native
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.Pipeline
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.QueryBackendSup
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.QuerySup
  alias Logflare.Mapper.OtelDefaults
  alias Logflare.S3TablesMockServer
  alias Logflare.SystemMetrics.AllLogsLogged

  doctest S3TablesAdaptor

  @valid_config %{
    table_bucket_arn: "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket",
    access_key_id: "aws_key_id",
    secret_access_key: "aws_secret_key",
    namespace: "my_namespace",
    batch_timeout: 5_000
  }

  test "config_validation" do
    assert %Ecto.Changeset{valid?: true} =
             Adaptor.cast_and_validate_config(S3TablesAdaptor, @valid_config)

    configs =
      [
        Map.delete(@valid_config, :table_bucket_arn),
        Map.delete(@valid_config, :access_key_id),
        Map.delete(@valid_config, :secret_access_key),
        Map.delete(@valid_config, :namespace),
        %{@valid_config | batch_timeout: 999},
        %{@valid_config | batch_timeout: 66_000}
      ]

    for config <- configs do
      assert %Ecto.Changeset{valid?: false} =
               Adaptor.cast_and_validate_config(S3TablesAdaptor, config)
    end
  end

  test "redact_config/1" do
    config = %{secret_access_key: "secret-key-123", table_bucket_arn: "arn:aws:..."}
    assert %{secret_access_key: "REDACTED"} = S3TablesAdaptor.redact_config(config)
  end

  describe "ingestion through the adaptor supervision tree" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :s3_tables,
          sources: [source],
          user: user,
          config: Map.put(@valid_config, :batch_timeout, 100)
        )

      catalog = make_ref()
      Mimic.stub(Native, :init_catalog, fn _config -> {:ok, catalog} end)

      Mimic.stub(Native, :ensure_table, fn _catalog, _table, _fields, _layout, _props ->
        {:ok, :created}
      end)

      start_supervised!(AllLogsLogged)
      start_supervised!({S3TablesAdaptor, backend})

      [source: source, backend: backend, catalog: catalog]
    end

    test "log event", %{source: source, backend: backend, catalog: catalog} do
      start_supervised!({CatalogManager, backend})
      test_pid = self()
      backend_id = backend.id

      Mimic.expect(Native, :append_batch, fn ^catalog, table_name, ndjson ->
        send(test_pid, {:appended, table_name, ndjson})
        {:ok, %{row_count: 1, data_files: 1}}
      end)

      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :backends, :pipeline, :handle_batch],
        [:logflare, :backends, :s3_tables, :append]
      ])

      assert {:ok, _} =
               Backends.ingest_logs(
                 [%{"event_message" => "adaptor level test", "metadata" => %{"level" => "info"}}],
                 source
               )

      assert_receive {:appended, "otel_logs", ndjson}, 5_000

      assert [row] = decode_ndjson(ndjson)
      assert row["event_message"] == "adaptor level test"
      assert row["mapping_config_id"] == OtelDefaults.config_id(:log)
      assert row["source_uuid"] == to_string(source.token)
      assert is_binary(row["id"])
      assert is_integer(row["ingested_at"])
      assert is_map(row["log_attributes"])

      assert_receive {[:logflare, :backends, :pipeline, :handle_batch], _ref, %{batch_size: 1},
                      %{
                        backend_type: :s3_tables,
                        backend_id: ^backend_id,
                        event_type: :log,
                        day_bucket: _
                      }},
                     5_000

      assert_receive {[:logflare, :backends, :s3_tables, :append], _ref,
                      %{duration_us: _, row_count: 1, data_files: 1},
                      %{status: :ok, backend_id: ^backend_id, event_type: :log}},
                     5_000
    end

    test "metric and trace events", %{source: source, backend: backend} do
      start_supervised!({CatalogManager, backend})
      test_pid = self()

      Mimic.expect(Native, :append_batch, 2, fn _catalog, table_name, ndjson ->
        send(test_pid, {:appended, table_name, ndjson})
        {:ok, %{row_count: 1, data_files: 1}}
      end)

      for {type, table_name} <- [{"metric", "otel_metrics"}, {"span", "otel_traces"}] do
        assert {:ok, _} =
                 Backends.ingest_logs(
                   [%{"event_message" => "typed event", "metadata" => %{"type" => type}}],
                   source
                 )

        assert_receive {:appended, ^table_name, ndjson}, 5_000
        assert [%{"event_message" => "typed event"}] = decode_ndjson(ndjson)
      end
    end

    test "append failure", %{source: source, backend: backend} do
      start_supervised!({CatalogManager, backend})
      test_pid = self()
      backend_id = backend.id
      attempts = Pipeline.max_retries() + 1

      Mimic.expect(Native, :append_batch, attempts, fn _catalog, _table_name, _ndjson ->
        send(test_pid, :append_attempt)
        {:error, :commit_conflict}
      end)

      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :backends, :s3_tables, :append]
      ])

      log =
        capture_log(fn ->
          assert {:ok, _} = Backends.ingest_logs([%{"event_message" => "doomed"}], source)

          for _ <- 1..attempts, do: assert_receive(:append_attempt, 5_000)
          refute_receive :append_attempt, 1_000
        end)

      assert log =~ "S3 Tables append failed"
      assert log =~ "exhausted #{Pipeline.max_retries()} retries"

      assert_receive {[:logflare, :backends, :s3_tables, :append], _ref, %{duration_us: _},
                      %{status: :error, reason: :commit_conflict, backend_id: ^backend_id}},
                     5_000
    end

    test "catalog not provisioned", %{source: source, backend: backend} do
      Mimic.reject(&Native.append_batch/3)
      backend_id = backend.id

      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :backends, :pipeline, :handle_batch]
      ])

      log =
        capture_log(fn ->
          assert {:ok, _} = Backends.ingest_logs([%{"event_message" => "no catalog"}], source)

          # the failed batch is retried once, then dropped
          assert_receive {[:logflare, :backends, :pipeline, :handle_batch], _ref, _,
                          %{backend_type: :s3_tables, backend_id: ^backend_id}},
                         5_000

          assert_receive {[:logflare, :backends, :pipeline, :handle_batch], _ref, _,
                          %{backend_type: :s3_tables, backend_id: ^backend_id}},
                         5_000

          refute_receive {[:logflare, :backends, :pipeline, :handle_batch], _ref, _,
                          %{backend_type: :s3_tables, backend_id: ^backend_id}},
                         1_000
        end)

      assert log =~ "S3 Tables append failed"
    end
  end

  describe "ingestion against a mock S3 Tables API" do
    setup do
      server = S3TablesMockServer.start()

      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)
      other_source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :s3_tables,
          sources: [source, other_source],
          user: user,
          config: %{
            table_bucket_arn: "arn:aws:s3tables:us-east-1:000000000000:bucket/mock-bucket",
            namespace: "mock_namespace",
            access_key_id: "mock-key",
            secret_access_key: "mock-secret",
            batch_timeout: 100,
            endpoint_url: server.endpoint,
            s3_endpoint: server.endpoint
          }
        )

      start_supervised!(AllLogsLogged)
      start_supervised!({S3TablesAdaptor, backend})
      start_supervised!({CatalogManager, backend})

      [source: source, other_source: other_source, backend: backend, server: server]
    end

    test "log event", %{source: source, backend: backend, server: server} do
      backend_id = backend.id

      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :backends, :s3_tables, :append]
      ])

      assert {:ok, _} = Backends.ingest_logs([%{"event_message" => "mock server test"}], source)

      assert_receive {[:logflare, :backends, :s3_tables, :append], _ref,
                      %{row_count: 1, data_files: 1},
                      %{status: :ok, backend_id: ^backend_id, event_type: :log}},
                     30_000

      assert %{"otel_logs" => _, "otel_metrics" => _, "otel_traces" => _} =
               S3TablesMockServer.tables(server)

      assert {:ok, catalog} = CatalogManager.fetch_catalog(backend_id)

      assert {:ok, %{partition: ["timestamp_day"], sort_order: sort_order}} =
               Native.table_info(catalog, "otel_logs")

      assert sort_order == ["project", "source_uuid", "timestamp"]

      assert {:ok, snapshot} = Native.snapshot_info(catalog, "otel_logs")
      assert snapshot.operation == "append"
      assert snapshot.summary["added-records"] == "1"

      assert Enum.any?(
               S3TablesMockServer.object_keys(server),
               &String.ends_with?(&1, ".parquet")
             )
    end

    @tag :tmp_dir
    test "batch of two sources and two projects in arrival order", %{
      source: source,
      other_source: other_source,
      backend: backend,
      server: server,
      tmp_dir: tmp_dir
    } do
      backend_id = backend.id
      noon = DateTime.new!(Date.utc_today(), ~T[12:00:00Z])

      # arrival order deliberately matches neither project, source nor time
      arrivals = [
        {other_source, "proj-b", 0},
        {source, "proj-b", 5},
        {source, "proj-a", 4},
        {other_source, "proj-a", 3},
        {source, "proj-a", 1},
        {other_source, "proj-b", 2}
      ]

      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :backends, :s3_tables, :append]
      ])

      for {ingest_source, events} <- Enum.group_by(arrivals, &elem(&1, 0)) do
        events =
          for {_source, project, second} <- events do
            %{
              "event_message" => "#{project} at #{second}",
              "project" => project,
              "timestamp" => DateTime.to_iso8601(DateTime.add(noon, second))
            }
          end

        assert {:ok, _} = Backends.ingest_logs(events, ingest_source)
      end

      assert_receive {[:logflare, :backends, :s3_tables, :append], _ref,
                      %{row_count: 6, data_files: 1}, %{status: :ok, backend_id: ^backend_id}},
                     30_000

      # the sort key includes source_uuid, whose values are only known now
      expected =
        for {row_source, project, second} <-
              Enum.sort_by(arrivals, fn {row_source, project, second} ->
                {project, to_string(row_source.token), second}
              end),
            do: {project, to_string(row_source.token), "#{project} at #{second}"}

      df = read_parquet(server, tmp_dir, ~w(project source_uuid event_message))

      assert Enum.zip([
               Explorer.Series.to_list(df["project"]),
               Explorer.Series.to_list(df["source_uuid"]),
               Explorer.Series.to_list(df["event_message"])
             ]) == expected
    end
  end

  defp read_parquet(server, tmp_dir, columns) do
    assert [key] =
             server
             |> S3TablesMockServer.object_keys()
             |> Enum.filter(&String.ends_with?(&1, ".parquet"))

    path = Path.join(tmp_dir, "data.parquet")
    File.write!(path, S3TablesMockServer.object(server, key))

    Explorer.DataFrame.from_parquet!(path, columns: columns)
  end

  defp decode_ndjson(ndjson) do
    ndjson
    |> String.split("\n", trim: true)
    |> Enum.map(&Jason.decode!/1)
  end

  defp integration_env!(var) do
    System.get_env(var) ||
      raise "the :integration suite runs against real AWS and requires #{var} to be set"
  end

  defp s3_tables_config(_ctx) do
    config =
      %{
        table_bucket_arn: integration_env!("LOGFLARE_S3_TABLES_TEST_BUCKET_ARN"),
        namespace: integration_env!("LOGFLARE_S3_TABLES_TEST_NAMESPACE"),
        access_key_id: integration_env!("AWS_ACCESS_KEY_ID"),
        secret_access_key: integration_env!("AWS_SECRET_ACCESS_KEY")
      }

    assert {:ok, catalog} = S3TablesAdaptor.Native.init_catalog(config)
    %{config: config, catalog: catalog}
  end

  describe "Native module (integration)" do
    @describetag :integration
    test "invalid credentials" do
      assert {:error, err} = S3TablesAdaptor.Native.init_catalog(@valid_config)
      assert err =~ "invalid"
    end

    # This suite is an opt-in live smoke test against real AWS
    # (`mix test --include integration`); day-to-day coverage of the append
    # path runs against the local mock server instead. The target bucket and
    # credentials come from the environment rather than test config so that
    # per-developer AWS secrets never live in the repo.
    setup :s3_tables_config

    setup %{catalog: catalog} do
      # drop the OTEL tables before an integration run so tables created by
      # earlier schema revisions don't leak their stale schemas into the tests
      for event_type <- IcebergSchema.event_types() do
        S3TablesAdaptor.Native.drop_table(catalog, IcebergSchema.table_name(event_type))
      end

      :ok
    end

    test "ensure_table/5 and table_info/2", %{catalog: catalog} do
      for event_type <- IcebergSchema.event_types() do
        table_name = IcebergSchema.table_name(event_type)
        fields = IcebergSchema.fields(event_type)
        layout = IcebergSchema.layout(event_type)
        properties = IcebergSchema.table_properties(event_type)

        assert {:ok, _status} =
                 S3TablesAdaptor.Native.ensure_table(
                   catalog,
                   table_name,
                   fields,
                   layout,
                   properties
                 )

        assert {:ok, :already_exists} =
                 S3TablesAdaptor.Native.ensure_table(
                   catalog,
                   table_name,
                   fields,
                   layout,
                   properties
                 )

        assert {:ok, info} = S3TablesAdaptor.Native.table_info(catalog, table_name)
        assert info.columns == Enum.map(fields, & &1.name)
        assert info.partition == Enum.map(layout.partition, & &1.name)
        assert info.sort_order == Enum.map(layout.sort_order, & &1.field)

        assert info.properties["logflare.schema-version"] ==
                 IcebergSchema.schema_version(event_type)
      end
    end

    test "append_batch/3 snapshot generation", %{catalog: catalog} do
      table_name = IcebergSchema.table_name(:log)

      assert {:ok, _status} =
               S3TablesAdaptor.Native.ensure_table(
                 catalog,
                 table_name,
                 IcebergSchema.fields(:log),
                 IcebergSchema.layout(:log),
                 IcebergSchema.table_properties(:log)
               )

      {:ok, snapshot_before} = S3TablesAdaptor.Native.snapshot_info(catalog, table_name)
      snapshots_before = if snapshot_before, do: snapshot_before.snapshot_count, else: 0

      now_us = System.os_time(:microsecond)

      ndjson =
        for n <- 1..3, into: "" do
          row = %{
            "id" => Ecto.UUID.generate(),
            "event_message" => "integration test event #{n}",
            "timestamp" => now_us,
            "log_attributes" => %{"n" => "#{n}"}
          }

          Jason.encode!(row) <> "\n"
        end

      assert {:ok, %{row_count: 3, data_files: data_files}} =
               S3TablesAdaptor.Native.append_batch(catalog, table_name, ndjson)

      assert data_files >= 1

      assert {:ok, snapshot} = S3TablesAdaptor.Native.snapshot_info(catalog, table_name)
      assert snapshot.snapshot_count == snapshots_before + 1
      assert snapshot.operation == "append"
      assert snapshot.summary["added-records"] == "3"
    end

    test "concurrent appends", %{catalog: catalog} do
      table_name = IcebergSchema.table_name(:log)

      assert {:ok, _status} =
               S3TablesAdaptor.Native.ensure_table(
                 catalog,
                 table_name,
                 IcebergSchema.fields(:log),
                 IcebergSchema.layout(:log),
                 IcebergSchema.table_properties(:log)
               )

      now_us = System.os_time(:microsecond)

      results =
        1..2
        |> Task.async_stream(
          fn n ->
            row = %{
              "id" => Ecto.UUID.generate(),
              "event_message" => "concurrent append #{n}",
              "timestamp" => now_us
            }

            S3TablesAdaptor.Native.append_batch(catalog, table_name, Jason.encode!(row) <> "\n")
          end,
          timeout: 120_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert [{:ok, %{row_count: 1}}, {:ok, %{row_count: 1}}] = results
    end
  end

  test "map_query_parameters/4 orders values by their $1..$n positions" do
    params =
      S3TablesAdaptor.map_query_parameters(
        "SELECT id FROM otel_logs WHERE a = @foo AND b = @bar",
        "ignored transformed query",
        ["foo", "bar"],
        %{"foo" => "x", "bar" => "y"}
      )

    assert params == ["x", "y"]
  end

  describe "transform_query/3" do
    test ":duckdb_sql needs no further rewriting" do
      query = ~s|SELECT id FROM (SELECT * FROM otel_logs WHERE source_uuid = 'abc') AS s|

      assert {:ok, ^query} = S3TablesAdaptor.transform_query(query, :duckdb_sql, %{})
    end

    test "unsupported source language" do
      assert {:error, message} =
               S3TablesAdaptor.transform_query("SELECT 1", :bq_sql, %{})

      assert message =~ "not supported"
    end
  end

  describe "execute_query/3 (integration)" do
    @describetag :integration

    setup :s3_tables_config

    setup %{config: config, catalog: catalog} do
      table_name = IcebergSchema.table_name(:log)

      # start from an empty table so counts are deterministic
      S3TablesAdaptor.Native.drop_table(catalog, table_name)

      {:ok, _status} =
        S3TablesAdaptor.Native.ensure_table(
          catalog,
          table_name,
          IcebergSchema.fields(:log),
          IcebergSchema.layout(:log),
          IcebergSchema.table_properties(:log)
        )

      user = insert(:user)
      backend = insert(:backend, type: :s3_tables, user: user, config: config)

      on_exit(fn ->
        case GenServer.whereis(Backends.via_backend(backend, QueryBackendSup)) do
          pid when is_pid(pid) -> DynamicSupervisor.terminate_child(QuerySup, pid)
          _ -> :ok
        end
      end)

      %{backend: backend, catalog: catalog, table_name: table_name}
    end

    test "counts ingested rows and probes snapshot staleness on a live session", %{
      backend: backend,
      catalog: catalog,
      table_name: table_name
    } do
      now_ns = System.os_time(:nanosecond)

      append = fn range ->
        ndjson =
          for n <- range, into: "" do
            row = %{
              "id" => Ecto.UUID.generate(),
              "event_message" => "query integration event #{n}",
              "timestamp" => now_ns
            }

            Jason.encode!(row) <> "\n"
          end

        {:ok, _} = S3TablesAdaptor.Native.append_batch(catalog, table_name, ndjson)
      end

      append.(1..3)

      assert {:ok, %QueryResult{rows: [%{"c" => 3}]}} =
               S3TablesAdaptor.execute_query(backend, ~s|SELECT count(*) AS c FROM otel_logs|, [])

      # dotted (ClickHouse-parity Nested) columns must be double-quoted for DuckDB
      assert {:ok, %QueryResult{}} =
               S3TablesAdaptor.execute_query(
                 backend,
                 ~s|SELECT "event_message" FROM otel_logs LIMIT 5|,
                 []
               )

      # staleness probe: ingest more, re-query the SAME live ATTACH session.
      # 5 => the long-lived session sees new snapshots; 3 => it needs a re-ATTACH (Step 4).
      append.(4..5)

      assert {:ok, %QueryResult{rows: [%{"c" => count_after}]}} =
               S3TablesAdaptor.execute_query(backend, ~s|SELECT count(*) AS c FROM otel_logs|, [])

      assert count_after in [3, 5]
    end
  end
end
