defmodule WireProtocol do
  @moduledoc """
  Module that handles all transforms and operations and provides structure for a kafka wire protocol request/response shape
  """

  @doc "takes the data and returns a response in kafka format based on the data."
  @spec response(binary() | nil) :: binary() | nil
  def response(data) do
    <<_size::32, _api_key::16, _api_version::16, correlation_id::32, _rest::binary>> = data

    # 2. Construct the Response Header (just the Correlation ID for this stage)
    response_header = <<correlation_id::32>>

    # 3. Calculate total response size (4 bytes for correlation_id)
    # Note: Later stages will append a response body here, increasing this size!
    response_size = byte_size(response_header)

    # 4. Assemble the final packet and send it back
    <<response_size::32, response_header::binary>>
  end
end
