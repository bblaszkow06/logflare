defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.QuerySessionTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.QueryBackendSup
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.QueryEngine
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.QuerySession
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.QuerySup

  # secret_access_key embeds a single quote to exercise SQL escaping
  @config %{
    table_bucket_arn: "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket",
    namespace: "my_namespace",
    access_key_id: "AKIA_EXAMPLE",
    secret_access_key: "sec'ret",
    batch_timeout: 1_000
  }

  describe "region_from_arn/1" do
    test "parses the region, raises on a malformed ARN" do
      assert QuerySession.region_from_arn(@config.table_bucket_arn) == "us-west-2"

      assert QuerySession.region_from_arn("arn:aws:s3tables:eu-central-1:1:bucket/x") ==
               "eu-central-1"

      assert_raise ArgumentError, fn -> QuerySession.region_from_arn("not-an-arn") end
    end
  end

  describe "bootstrap_statements/1" do
    test "ordered statements with escaped secrets and quoted identifiers" do
      assert [
               "INSTALL aws",
               "INSTALL httpfs",
               "INSTALL iceberg",
               "LOAD aws",
               "LOAD httpfs",
               "LOAD iceberg",
               create_secret,
               attach,
               use_statement,
               memory_limit
             ] = QuerySession.bootstrap_statements(@config)

      assert create_secret =~ "CREATE OR REPLACE SECRET"
      assert create_secret =~ "KEY_ID 'AKIA_EXAMPLE'"
      assert create_secret =~ "SECRET 'sec''ret'"
      assert create_secret =~ "REGION 'us-west-2'"
      # the raw, unescaped secret must never appear
      refute create_secret =~ ~r/SECRET 'sec'ret'/

      assert attach =~
               "ATTACH IF NOT EXISTS 'arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket' AS s3t"

      assert attach =~ "TYPE iceberg, ENDPOINT_TYPE s3_tables"
      assert use_statement == ~s(USE s3t."my_namespace")
      assert memory_limit =~ "SET memory_limit"
    end
  end

  describe "execute/4" do
    setup do
      user = insert(:user)
      backend = insert(:backend, type: :s3_tables, user: user, config: @config)

      # gracefully stop the per-backend subtree (Adbc.Database, Adbc.Connection, QuerySession)
      on_exit(fn ->
        case GenServer.whereis(Backends.via_backend(backend, QueryBackendSup)) do
          pid when is_pid(pid) -> DynamicSupervisor.terminate_child(QuerySup, pid)
          _ -> :ok
        end
      end)

      %{backend: backend}
    end

    test "lazily bootstraps once, reuses the connection, and shapes rows into column maps", %{
      backend: backend
    } do
      test_pid = self()

      # only the S3-touching SQL is mocked; the Adbc.Database/Connection start for real
      Mimic.stub(QueryEngine, :execute, fn _conn, sql, _params, _opts ->
        send(test_pid, {:executed, sql})

        if String.starts_with?(sql, "SELECT") do
          {:ok, %{columns: ["c", "name"], rows: [[1, "a"], [2, "b"]]}}
        else
          {:ok, %{columns: [], rows: []}}
        end
      end)

      # lazy: nothing started until the first query
      refute GenServer.whereis(Backends.via_backend(backend, QueryBackendSup))

      assert {:ok, %QueryResult{rows: rows}} =
               QuerySession.execute(backend, "SELECT count(*) AS c FROM otel_logs", [])

      assert is_pid(GenServer.whereis(Backends.via_backend(backend, QueryBackendSup)))
      assert rows == [%{"c" => 1, "name" => "a"}, %{"c" => 2, "name" => "b"}]

      # every bootstrap statement ran, in order, ahead of the query
      for statement <- QuerySession.bootstrap_statements(@config) do
        assert_receive {:executed, ^statement}
      end

      assert_receive {:executed, "SELECT count(*) AS c FROM otel_logs"}

      # a second query reuses the connection — no re-bootstrap
      assert {:ok, %QueryResult{}} = QuerySession.execute(backend, "SELECT 1", [])
      assert_receive {:executed, "SELECT 1"}
      refute_receive {:executed, "INSTALL aws"}
    end

    test "surfaces bootstrap failures and retries on the next call", %{
      backend: backend
    } do
      Mimic.expect(QueryEngine, :execute, fn _conn, _sql, _params, _opts ->
        {:error, :boom}
      end)

      assert {:error, :boom} = QuerySession.execute(backend, "SELECT 1", [])

      # connection was left un-bootstrapped; the next call re-bootstraps and succeeds
      Mimic.stub(QueryEngine, :execute, fn _conn, _sql, _params, _opts ->
        {:ok, %{columns: [], rows: []}}
      end)

      assert {:ok, %QueryResult{}} = QuerySession.execute(backend, "SELECT 1", [])
    end
  end
end
