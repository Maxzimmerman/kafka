defmodule WireProtocol do
  @moduledoc """
  Module that handles all transforms and operations and provides structure for a kafka wire protocol request/response shape
  """

  @valid_versions [4]
  @error_codes %{UNSUPPORTED_VERSION: 35}

  @doc "takes the data and returns a response in kafka format based on the data."
  @spec response(binary() | nil) :: binary() | nil
  def response(data) do
    <<_size::32, _api_key::16, api_version::16, correlation_id::32, _rest::binary>> = data
    IO.inspect(api_version, label: "API VERSION")
    IO.inspect(error_code_field(api_version))
    IO.inspect(<<error_code_field(api_version)::16>>)

    # 2. Construct the Response Header (just the Correlation ID for this stage)
    response_header = <<correlation_id::32, error_code_field(api_version)>>

    # 3. Calculate total response size (4 bytes for correlation_id)
    # Note: Later stages will append a response body here, increasing this size!
    response_size = byte_size(response_header)

    # 4. Assemble the final packet and send it back
    <<response_size::32, response_header::binary>>
  end

  defp validate_api_version(version), do: version in @valid_versions

  defp error_code_field(api_version) do
    if validate_api_version(api_version) do
      0
    else
      35
    end
  end
end
