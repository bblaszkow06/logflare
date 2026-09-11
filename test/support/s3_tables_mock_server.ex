defmodule Logflare.S3TablesMockServer do
  @moduledoc """
  In-memory mock of the AWS S3 Tables REST API plus the S3 object API, served
  by Bandit on localhost.

  Implements just enough surface for the `s3_tables_ex` NIF to run its full
  catalog + Iceberg append path without AWS: `GetNamespace`, `GetTable`,
  `CreateTable`, `UpdateTableMetadataLocation` (with version-token conflict
  detection) and `DeleteTable` on the catalog side, and `PUT`/`GET`/`HEAD`/
  `DELETE` objects plus multipart uploads on the object side. Request
  signatures are not verified.

  Start it with `start/0` and point the backend config's `endpoint_url` and
  `s3_endpoint` at the returned endpoint.
  """

  @behaviour Plug

  import Plug.Conn

  require Logger

  # single flat bucket standing in for the per-table warehouse buckets AWS
  # generates; table locations are prefixed with the table name
  @bucket "warehouse"
  @account_id "000000000000"
  @timestamp "2026-01-01T00:00:00Z"

  @type server :: %{agent: pid(), port: pos_integer(), endpoint: String.t()}

  @doc """
  Starts the state agent and the Bandit listener, both linked to the caller.

  Returns the server handle with the HTTP `endpoint` to use for both the
  `endpoint_url` and `s3_endpoint` backend config keys.
  """
  @spec start() :: server()
  def start do
    {:ok, agent} =
      Agent.start_link(fn -> %{tables: %{}, objects: %{}, uploads: %{}, token_seq: 0} end)

    {:ok, listener} =
      Bandit.start_link(
        plug: {__MODULE__, {:agent, agent}},
        port: 0,
        ip: :loopback,
        startup_log: false
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(listener)

    %{agent: agent, port: port, endpoint: "http://127.0.0.1:#{port}"}
  end

  @doc "Returns the created tables as `%{name => %{version_token, metadata_location}}`."
  @spec tables(server()) :: %{String.t() => map()}
  def tables(%{agent: agent}), do: Agent.get(agent, & &1.tables)

  @doc "Returns the keys of all stored S3 objects."
  @spec object_keys(server()) :: [String.t()]
  def object_keys(%{agent: agent}), do: Agent.get(agent, &Map.keys(&1.objects))

  @doc "Returns the body of a stored S3 object, or `nil` when the key is unknown."
  @spec object(server(), String.t()) :: binary() | nil
  def object(%{agent: agent}, key), do: Agent.get(agent, &Map.get(&1.objects, key))

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, {:agent, agent}) do
    conn = fetch_query_params(conn)
    {body, conn} = read_full_body(conn)
    route(conn, conn.method, conn.path_info, body, agent)
  end

  # --- S3 Tables catalog API ---

  defp route(conn, "GET", ["get-table"], _body, agent) do
    name = conn.query_params["name"]
    namespace = conn.query_params["namespace"]

    case Agent.get(agent, &Map.get(&1.tables, name)) do
      nil ->
        aws_error(conn, 404, "NotFoundException")

      table ->
        json(conn, 200, %{
          "name" => name,
          "type" => "customer",
          "tableARN" => table_arn(name),
          "namespace" => [namespace],
          "versionToken" => table.version_token,
          "metadataLocation" => table.metadata_location,
          "warehouseLocation" => "s3://#{@bucket}/#{name}",
          "createdAt" => @timestamp,
          "createdBy" => @account_id,
          "modifiedAt" => @timestamp,
          "modifiedBy" => @account_id,
          "ownerAccountId" => @account_id,
          "format" => "ICEBERG"
        })
    end
  end

  defp route(conn, "GET", ["namespaces", _arn, namespace], _body, _agent) do
    json(conn, 200, %{
      "namespace" => [namespace],
      "createdAt" => @timestamp,
      "createdBy" => @account_id,
      "ownerAccountId" => @account_id
    })
  end

  defp route(conn, "PUT", ["tables", _arn, _namespace], body, agent) do
    %{"name" => name} = Jason.decode!(body)

    created =
      Agent.get_and_update(agent, fn state ->
        if Map.has_key?(state.tables, name) do
          {nil, state}
        else
          {token, state} = next_token(state)
          table = %{version_token: token, metadata_location: nil}
          {table, put_in(state.tables[name], table)}
        end
      end)

    case created do
      nil ->
        aws_error(conn, 409, "ConflictException")

      table ->
        json(conn, 200, %{"tableARN" => table_arn(name), "versionToken" => table.version_token})
    end
  end

  defp route(conn, "PUT", ["tables", _arn, namespace, name, "metadata-location"], body, agent) do
    %{"metadataLocation" => location, "versionToken" => token} = Jason.decode!(body)

    updated =
      Agent.get_and_update(agent, fn state ->
        case Map.get(state.tables, name) do
          %{version_token: ^token} ->
            {new_token, state} = next_token(state)
            table = %{version_token: new_token, metadata_location: location}
            {table, put_in(state.tables[name], table)}

          _missing_or_stale ->
            {nil, state}
        end
      end)

    case updated do
      nil ->
        aws_error(conn, 409, "ConflictException")

      table ->
        json(conn, 200, %{
          "name" => name,
          "tableARN" => table_arn(name),
          "namespace" => [namespace],
          "metadataLocation" => location,
          "versionToken" => table.version_token
        })
    end
  end

  defp route(conn, "DELETE", ["tables", _arn, _namespace, name], _body, agent) do
    Agent.update(agent, fn state -> %{state | tables: Map.delete(state.tables, name)} end)
    send_resp(conn, 204, "")
  end

  # --- S3 object API (path-style requests against the warehouse bucket) ---

  defp route(conn, method, [@bucket | key_parts], body, agent) do
    s3(conn, method, Enum.join(key_parts, "/"), body, agent)
  end

  defp route(conn, method, path_info, _body, _agent) do
    Logger.warning("S3TablesMockServer: unhandled #{method} /#{Enum.join(path_info, "/")}")
    send_resp(conn, 501, "")
  end

  defp s3(conn, "PUT", key, body, agent) do
    case conn.query_params do
      %{"partNumber" => part_number, "uploadId" => upload_id} ->
        Agent.update(agent, fn state ->
          put_in(state.uploads[upload_id].parts[String.to_integer(part_number)], body)
        end)

        conn |> put_resp_header("etag", etag(body)) |> send_resp(200, "")

      _plain_put ->
        Agent.update(agent, &put_in(&1.objects[key], body))
        conn |> put_resp_header("etag", etag(body)) |> send_resp(200, "")
    end
  end

  defp s3(conn, "POST", key, _body, agent) do
    cond do
      Map.has_key?(conn.query_params, "uploads") ->
        upload_id = Ecto.UUID.generate()
        Agent.update(agent, &put_in(&1.uploads[upload_id], %{key: key, parts: %{}}))

        xml(conn, 200, """
        <?xml version="1.0" encoding="UTF-8"?>
        <InitiateMultipartUploadResult>
          <Bucket>#{@bucket}</Bucket><Key>#{key}</Key><UploadId>#{upload_id}</UploadId>
        </InitiateMultipartUploadResult>
        """)

      upload_id = conn.query_params["uploadId"] ->
        object =
          Agent.get_and_update(agent, fn state ->
            %{parts: parts} = Map.fetch!(state.uploads, upload_id)

            object =
              parts |> Enum.sort_by(&elem(&1, 0)) |> Enum.map_join("", &elem(&1, 1))

            state = %{state | uploads: Map.delete(state.uploads, upload_id)}
            {object, put_in(state.objects[key], object)}
          end)

        xml(conn, 200, """
        <?xml version="1.0" encoding="UTF-8"?>
        <CompleteMultipartUploadResult>
          <Location>http://127.0.0.1/#{@bucket}/#{key}</Location>
          <Bucket>#{@bucket}</Bucket><Key>#{key}</Key><ETag>#{etag(object)}</ETag>
        </CompleteMultipartUploadResult>
        """)
    end
  end

  defp s3(conn, "GET", key, _body, agent) do
    case Agent.get(agent, &Map.get(&1.objects, key)) do
      nil ->
        no_such_key(conn)

      object ->
        {status, range} = apply_range(conn, object)

        conn
        |> put_resp_content_type("application/octet-stream")
        |> put_resp_header("etag", etag(object))
        |> send_resp(status, range)
    end
  end

  defp s3(conn, "HEAD", key, _body, agent) do
    case Agent.get(agent, &Map.get(&1.objects, key)) do
      nil ->
        send_resp(conn, 404, "")

      object ->
        conn
        |> put_resp_header("content-length", Integer.to_string(byte_size(object)))
        |> put_resp_header("etag", etag(object))
        |> send_resp(200, "")
    end
  end

  defp s3(conn, "DELETE", key, _body, agent) do
    case conn.query_params do
      %{"uploadId" => upload_id} ->
        Agent.update(agent, fn state ->
          %{state | uploads: Map.delete(state.uploads, upload_id)}
        end)

      _plain_delete ->
        Agent.update(agent, fn state -> %{state | objects: Map.delete(state.objects, key)} end)
    end

    send_resp(conn, 204, "")
  end

  # --- helpers ---

  defp apply_range(conn, object) do
    with [range_header] <- get_req_header(conn, "range"),
         %{"first" => first, "last" => last} <-
           Regex.named_captures(~r/bytes=(?<first>\d+)-(?<last>\d*)/, range_header) do
      first = String.to_integer(first)
      last = if last == "", do: byte_size(object) - 1, else: String.to_integer(last)
      {206, binary_part(object, first, min(last, byte_size(object) - 1) - first + 1)}
    else
      _no_range -> {200, object}
    end
  end

  defp next_token(state) do
    seq = state.token_seq + 1
    {"token-#{seq}", %{state | token_seq: seq}}
  end

  defp table_arn(name) do
    "arn:aws:s3tables:us-east-1:#{@account_id}:bucket/mock-bucket/table/#{name}"
  end

  defp etag(body), do: ~s("#{Base.encode16(:erlang.md5(body), case: :lower)}")

  defp read_full_body(conn, acc \\ []) do
    case read_body(conn, length: 64_000_000) do
      {:ok, body, conn} -> {IO.iodata_to_binary([acc, body]), conn}
      {:more, part, conn} -> read_full_body(conn, [acc, part])
    end
  end

  defp json(conn, status, payload) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(payload))
  end

  defp xml(conn, status, payload) do
    conn
    |> put_resp_content_type("application/xml")
    |> send_resp(status, payload)
  end

  defp aws_error(conn, status, type) do
    conn
    |> put_resp_header("x-amzn-errortype", type)
    |> json(status, %{"__type" => type, "message" => type})
  end

  defp no_such_key(conn) do
    xml(conn, 404, """
    <?xml version="1.0" encoding="UTF-8"?>
    <Error><Code>NoSuchKey</Code><Message>NoSuchKey</Message></Error>
    """)
  end
end
