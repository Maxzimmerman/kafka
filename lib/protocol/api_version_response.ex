defmodule Kafka.Protocol.ApiVersionsRequest do
  @moduledoc """
  Represents a Kafka ApiVersions Request (typically API Key 18).
  """

  # The main request wraps the header and the body
  defstruct [
    :size,
    :header,
    :body
  ]

  defmodule Header do
    @moduledoc """
    Kafka Request Header (typically Version 2+ for flexible versions).
    """
    defstruct [
      :correlation_id,      # 4-byte integer
    ]
  end

  defmodule Body do
    @moduledoc """
    The specific body fields for an ApiVersions Request.
    """
    defstruct [
      :client_software_name,     # Compact string (e.g., "kafka-cli")
      :client_software_version,  # Compact string (e.g., "0.1")
      tag_buffer: 0x00           # 1-byte component for flexible fields
    ]
  end
end
