defmodule Kafka.Protocol.ApiVersionsRequest do
  @moduledoc """
  Represents a Kafka ApiVersions Request (typically API Key 18).
  """

  @type t :: %__MODULE__{
          size: String.t(),
          header: Header.t(),
          body: Body.t()
        }

  # The main request wraps the header and the body
  defstruct [
    :size,
    :header,
    :body
  ]

  defmodule Header do
    @type t :: %__MODULE__{
            correlation_id: integer()
          }

    @moduledoc """
    Kafka Request Header (typically Version 2+ for flexible versions).
    """
    defstruct [
      # 2-byte integer (18 for ApiVersions)
      :api_key,
      # 2-byte integer
      :api_version,
      # 4-byte integer
      :correlation_id,
      # String (e.g., "kafka-cli")
      :client_id,
      # 1-byte component for flexible fields
      tag_buffer: 0x00
    ]
  end

  defmodule Body do
    @moduledoc """
    The specific body fields for an ApiVersions Request.
    """
    defstruct [
      # Compact string (e.g., "kafka-cli")
      :client_software_name,
      # Compact string (e.g., "0.1")
      :client_software_version,
      # 1-byte component for flexible fields
      tag_buffer: 0x00
    ]
  end
end
