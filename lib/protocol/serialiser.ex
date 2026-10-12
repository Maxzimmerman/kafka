defmodule Kafka.Protocol.Serialiser do
  @moduledoc """
  Module that handles all transforms and operations and provides structure for a kafka wire protocol request/response shape
  """

  alias Kafka.Protocol.ApiVersionsRequest

  @valid_versions [4]

  # ------------------------------------ INTERFACE ------------------------------------

  @doc "Receives request as struct converts to response bytes"
  @spec request_struct_to_response_bytes(ApiVersionsRequest.t() | nil) :: {:ok, binary()} | nil
  def request_struct_to_response_bytes(%ApiVersionsRequest{
        header: %ApiVersionsRequest.Header{
          correlation_id: correlation_id,
          api_version: api_version
        },
        body: %ApiVersionsRequest.Body{}
      }) do
    with {:ok, response_header} <- response_header(correlation_id, api_version),
         {:ok, response_size} <- response_size(response_header) do
      {:ok, <<response_size::32, response_header::binary>>}
    end
  end

  @doc "Receives request bytes and converts to request struct"
  def request_bytes_to_struct(<<data::binary>>) do
    with %ApiVersionsRequest{
           header: %ApiVersionsRequest.Header{},
           body: %ApiVersionsRequest.Body{}
         } = request_struct <-
           request_to_struct(data) do
      request_struct
    end
  end

  # ------------------------------------ REQUEST HELPERS ------------------------------------

  defp request_to_struct(
         <<size::32, api_key::16, api_version::16, correlation_id::32, rest::binary>>
       ) do
    {client_id, rest} = extract_bytes_with_length(2, rest)
    {client_software_name, rest} = extract_bytes_with_length(2, rest)
    {client_software_version, rest} = extract_bytes_with_length(2, rest)

    IO.inspect(%ApiVersionsRequest{
      size: size,
      header: %ApiVersionsRequest.Header{
        api_key: api_key,
        api_version: api_version,
        correlation_id: correlation_id,
        client_id: client_id,
        tag_buffer: nil
      },
      body: %ApiVersionsRequest.Body{}
    })
  end

  # header 

  defp extract_bytes_with_length(2, <<length::16, rest::binary>>) do
    IO.inspect(length)
    <<content::binary-size(length), rest::binary>> = rest
    {content, rest}
  end

  defp extract_bytes_with_length(1, <<length::8, rest::binary>>) do
    <<content::binary-size(length), rest::binary>> = rest
    {content, rest}
  end

  defp extract_tag_buffer(<<buffer::8, rest::binary>>), do: {buffer, rest}

  # body

  # ------------------------------------ RESPONSE HELPERS ------------------------------------

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
