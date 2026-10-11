defmodule Kafka.Protocol.ApiVersionsResponse do
  @moduledoc """
  Represents a Kafka ApiVersions Response.
  """
  defstruct [
    :header,
    :body
  ]

  defmodule Header do
    defstruct [
      :correlation_id  # 4-byte big-endian integer echoed from request
    ]
  end

  defmodule Body do
    defstruct [
      :error_code,     # 2-byte integer (0 means success)
      :api_keys,       # List of %ApiKeySupport{} structs
      :throttle_time,  # 4-byte integer (duration in ms client should slow down)
      tag_buffer: 0x00 # 1-byte component for flexible fields
    ]
  end

  defmodule ApiKeySupport do
    defstruct [
      :api_key,        # 2-byte integer identifying the API (e.g., Produce, Fetch)
      :min_version,    # 2-byte integer
      :max_version,    # 2-byte integer
      tag_buffer: 0x00 # 1-byte component for flexible fields
    ]
  end
end
