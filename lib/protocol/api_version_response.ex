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
      # 4-byte big-endian integer echoed from request
      :correlation_id
    ]
  end

  defmodule Body do
    defstruct [
      # 2-byte integer (0 means success)
      :error_code,
      # List of %ApiKeySupport{} structs
      :api_keys,
      # 4-byte integer (duration in ms client should slow down)
      :throttle_time,
      # 1-byte component for flexible fields
      tag_buffer: 0x00
    ]
  end

  defmodule ApiKeySupport do
    defstruct [
      # 2-byte integer identifying the API (e.g., Produce, Fetch)
      :api_key,
      # 2-byte integer
      :min_version,
      # 2-byte integer
      :max_version,
      # 1-byte component for flexible fields
      tag_buffer: 0x00
    ]
  end
end
