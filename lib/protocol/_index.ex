defmodule Kafka.Protocol do
  @moduledoc """
  Protocol Orchestrator
  """
  alias Kafka.Protocol.Serialiser
  alias Kafka.Protocol.ApiVersionsRequest

  @doc "takes request bytes and returns response bytes"
  def response(data) do
    with %ApiVersionsRequest{} = request_struct <- Serialiser.request_bytes_to_struct(data),
         {:ok, response_bytes} <- Serialiser.request_struct_to_response_bytes(request_struct) do
      response_bytes
    end
  end
end
