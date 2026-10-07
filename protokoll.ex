defmodule Protocol do
@moduledoc """
Module that handles all transforms and operations and provides structure for a kafka wire protocol request/response shape
"""

  def response_size, do: <<2::integer-size(32)-big>>
end
