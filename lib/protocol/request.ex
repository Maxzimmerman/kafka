defmodule WireProtocol.Request do
  defstruct [:size, :api_key, :api_version, :correlation_id, :rest]
end
