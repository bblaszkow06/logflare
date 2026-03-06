defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient do
  @moduledoc false

  alias Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest
  alias Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.ArrowData
  alias Google.Cloud.Bigquery.Storage.V1.ArrowRecordBatch
  alias Google.Cloud.Bigquery.Storage.V1.BigQueryWrite
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC
  alias Logflare.Networking.GrpcPool

  require Logger

  @spec connetion_pool_name() :: module()
  def connetion_pool_name(), do: GrpcPool

  @spec append_rows({:arrow, list()}, keyword(), String.t()) :: :ok | {:error, term()}
  def append_rows({:arrow, data_frame}, context, table) do
    project = context[:project_id]
    dataset = context[:dataset_id]

    with {:ok, channel} <- GrpcPool.get_channel(connetion_pool_name()) do
      {arrow_schema, batch_msgs} =
        data_frame
        |> Enum.map_join("\n", &Jason.encode!/1)
        |> ArrowIPC.get_ipc_bytes()

      writer_schema = %Google.Cloud.Bigquery.Storage.V1.ArrowSchema{
        serialized_schema: arrow_schema
      }

      stream = BigQueryWrite.Stub.append_rows(channel)

      if length(batch_msgs) > 1 do
        Logger.warning("Storage Write ArrowIPC.get_ipc_bytes produced more than one batch message")
      end

      stream =
        Enum.reduce(batch_msgs, stream, fn ipc_msg, stream ->
          arrow_record_batch = %ArrowRecordBatch{
            serialized_record_batch: ipc_msg
          }

          arrow_rows = %ArrowData{rows: arrow_record_batch, writer_schema: writer_schema}

          request =
            %AppendRowsRequest{
              write_stream:
                "projects/#{project}/datasets/#{dataset}/tables/#{table}/streams/_default",
              rows: {:arrow_rows, arrow_rows}
            }

          GRPC.Stub.send_request(stream, request)
        end)

      GRPC.Stub.end_stream(stream)

      GRPC.Stub.recv(stream)
      |> case do
        {:ok, responses} ->
          Enum.each(responses, fn
            {:error, response} ->
              Logger.warning(
                "Storage Write API AppendRows response error - #{inspect(response)}"
              )

            {:ok, %{response: {:error, %{message: msg}}}} ->
              Logger.warning(
                "Storage Write API AppendRows response with error msg - #{inspect(msg)}"
              )

              :ok

            _ ->
              :ok
          end)

          :ok

        {:error, response} = err ->
          Logger.warning("Storage Write API AppendRows  error - #{inspect(response)}")
          err
      end
    else
      {:error, :not_connected} ->
        Logger.warning("GrpcPool: no channel available for BigQuery Storage Write")
        {:error, :grpc_not_connected}
    end
  end
end
