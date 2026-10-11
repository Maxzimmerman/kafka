defmodule Kafka.Protocol do
  @moduledoc """
  Module that handles all transforms and operations and provides structure for a kafka wire protocol request/response shape
  """

  alias Kafka.Protocol.ApiVersionsResponse
  alias Kafka.Protocol.ApiVersionsRequest

  @valid_versions [4]
  @error_codes %{UNSUPPORTED_VERSION: 35}

  # ------------------------------------ INTERFACE ------------------------------------

  @doc "Receives request as struct converts to response bytes"
  @spec response(binary() | nil) :: binary() | nil
  def response(%ApiVersionsRequest{
        header: %ApiVersionsRequest.Header{
          correlation_id: correlation_id,
          api_version: api_version
        },
        body: %ApiVersionsRequest.Body{}
      }) do
    with {:ok, response_header} <- response_header(correlation_id, api_version),
         {:ok, response_size} <- response_size(response_header) do
      # 2. Construct the Response Header (just the Correlation ID for this stage)

      # 3. Calculate total response size (4 bytes for correlation_id)
      # Note: Later stages will append a response body here, increasing this size!

      # 4. Assemble the final packet and send it back
      <<response_size::32, response_header::binary>>
    end
  end

  @doc "Receives request bytes and converts to request struct"
  def request(<<data::binary>>) do
    with %ApiVersionsRequest{
           header: %ApiVersionsRequest.Header{},
           body: %ApiVersionsRequest.Body{} = request_struct
         } <-
           request_to_struct(data) do
      request_struct
    end
  end

  # ------------------------------------ REQUEST ------------------------------------

  defp request_to_struct(
         <<size::32, api_key::16, api_version::16, correlation_id::32, _rest::binary>>
       ),
       do: %ApiVersionsRequest{
         size: size,
         header: %ApiVersionsRequest.Header{
           api_key: api_key,
           api_version: api_version,
           correlation_id: correlation_id
         },
         body: %ApiVersionsRequest.Body{}
       }

  # ------------------------------------ RESPONSE ------------------------------------

  defp response_header(correlation_id, api_version) do
    {:ok, <<correlation_id::32, error_code_field(api_version)::16>>}
  end

  defp response_size(response_header), do: {:ok, byte_size(response_header)}

  defp validate_api_version(version), do: version in @valid_versions

  defp error_code_field(api_version) do
    if validate_api_version(api_version) do
      0
    else
      35
    end
  end
end
