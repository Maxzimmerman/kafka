defmodule Kafka.Protocol.ApiVersionsResponse do
  @moduledoc """
  Represents a Kafka ApiVersions Response.
  """

  @type t :: %__MODULE__{
          header: __MODULE__.Header.t(),
          body: __MODULE__.Body.t()
        }

  defstruct [
    :header,
    :body
  ]

  defmodule Header do
    @type t :: %__MODULE__{
            correlation_id: integer()
          }

    defstruct [
      # 4-byte big-endian integer echoed from request
      :correlation_id
    ]
  end

  defmodule Body do
    @type t :: %__MODULE__{
            error_code: integer(),
            api_keys: list(__MODULE__.ApiKeySupport.t()),
            throttle_time: integer(),
            tag_buffer: integer()
          }

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
    @type t :: %__MODULE__{
            api_key: integer(),
            min_version: integer(),
            max_version: integer(),
            tag_buffer: integer()
          }

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
