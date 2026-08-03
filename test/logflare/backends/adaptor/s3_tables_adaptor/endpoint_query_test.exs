defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.EndpointQueryTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.S3TablesAdaptor
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.QueryBackendSup
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.QueryEngine
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.QuerySup
  alias Logflare.Backends.QueryError
  alias Logflare.Endpoints
  alias LogflareWeb.QueryErrorHelpers

  @config %{
    table_bucket_arn: "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket",
    namespace: "my_namespace",
    access_key_id: "AKIA_EXAMPLE",
    secret_access_key: "secret"
  }

  setup do
    # Sql.transform/3 resolves source retention, which needs a Free plan
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user, name: "my_otel_source")
    backend = insert(:backend, type: :s3_tables, user: user, config: @config)

    endpoint =
      insert(:endpoint,
        user: user,
        backend: backend,
        language: :duckdb_sql,
        query: "SELECT count(*) AS c FROM my_otel_source WHERE service_name = @svc"
      )

    on_exit(fn ->
      case GenServer.whereis(Backends.via_backend(backend, QueryBackendSup)) do
        pid when is_pid(pid) -> DynamicSupervisor.terminate_child(QuerySup, pid)
        _ -> :ok
      end
    end)

    %{source: source, endpoint: endpoint}
  end

  test "runs a :duckdb_sql endpoint end-to-end: filters by source_uuid, maps @param to $1", %{
    source: source,
    endpoint: endpoint
  } do
    test_pid = self()

    # only the S3-touching SQL is mocked; the Adbc.Database/Connection start for real
    Mimic.stub(QueryEngine, :execute, fn _conn, sql, params, _opts ->
      send(test_pid, {:executed, sql, params})

      if String.starts_with?(sql, "SELECT") do
        {:ok, %{columns: ["c"], rows: [[42]]}}
      else
        {:ok, %{columns: [], rows: []}}
      end
    end)

    assert {:ok, %{rows: [%{"c" => 42}]}} = Endpoints.run_query(endpoint, %{"svc" => "api"})

    assert_receive {:executed, select_sql, ["api"]}
    assert select_sql =~ ~s|FROM (SELECT * FROM otel_logs WHERE source_uuid = '#{source.token}')|
    assert select_sql =~ "service_name = $1"
    refute select_sql =~ "log_events_"
  end

  test "wraps a DuckDB error into a %QueryError{} the UI can render", %{endpoint: endpoint} do
    parser_error = %Adbc.Error{
      message: "Parser Error: syntax error at or near \"`\"",
      vendor_code: 0
    }

    Mimic.stub(QueryEngine, :execute, fn _conn, sql, _params, _opts ->
      if String.starts_with?(sql, "SELECT"),
        do: {:error, parser_error},
        else: {:ok, %{columns: [], rows: []}}
    end)

    backend = Backends.get_backend(endpoint.backend_id)

    assert {:error, %QueryError{kind: :invalid_query, backend: S3TablesAdaptor} = query_error} =
             S3TablesAdaptor.execute_query(backend, "SELECT 1", [])

    # the UI error helper must render it (previously crashed on a raw %Adbc.Error{})
    assert is_binary(QueryErrorHelpers.query_error_message(query_error))
  end
end
