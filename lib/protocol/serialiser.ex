defmodule Kafka.Protocol.Serialiser do
  @moduledoc """
  Module that handles all transforms and operations and provides structure for a kafka wire protocol request/response shape
  """
  import Bitwise

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
    # client_id is a classic NULLABLE_STRING in the header: INT16 length prefix.
    {client_id, rest} = extract_string(2, rest)

    # The (v2/flexible) header ends with a TAG_BUFFER — one 0x00 byte here. Skip it.
    <<_header_tag_buffer::8, rest::binary>> = rest

    # Body fields are COMPACT_STRINGs: length is an UNSIGNED_VARINT of (N + 1).
    {client_software_name, rest} = extract_compact_string(rest)
    {client_software_version, _rest} = extract_compact_string(rest)

    IO.inspect(%ApiVersionsRequest{
      size: size,
      header: %ApiVersionsRequest.Header{
        api_key: api_key,
        api_version: api_version,
        correlation_id: correlation_id,
        client_id: client_id
      },
      body: %ApiVersionsRequest.Body{
        client_software_name: client_software_name,
        client_software_version: client_software_version
      }
    })
  end

  # header

  # =========================================================================================================================
  # KAFKA COMPACT_STRING BIT-BY-BIT PARSING TRACE
  # =========================================================================================================================
  #
  # Initial Full Payload:
  # <<10, 107, 97, 102, 107, 97, 45, 99, 108, 105, 4, 48, 46, 49, 0>>
  # 
  # Initial Bit Layout:
  # [Varint 1] [------------------------- "kafka-cli" -------------------------] [Varint 2] [------- "0.1" -------] [Tag Buf]
  # 0000 1010  0110 1011 0110 0001 0110 0110 0110 1011 0110 0001 0010 1101 0110 0011 0110 1100 0110 1001  0000 0100  0011 0000 0011 0100 0011 0001  0000 0000
  #
  # -------------------------------------------------------------------------------------------------------------------------
  # LINE 1: {client_software_name, rest} = extract_compact_string(rest)
  # -------------------------------------------------------------------------------------------------------------------------
  #
  #   1. Read Varint Byte (Length Prefix):
  #      Bits: 0000 1010 (Decimal 10)
  #            ^
  #            |__ MSB (Most Significant Bit) is 0. This signals the Varint decoding is complete.
  #
  #   2. Perform Bitwise Reassembly:
  #      - Extract 7 payload bits:  000 1010 -> 10
  #      - Shift and Accumulate:    (10 <<< 0) ||| 0 = 10
  #      - Target String Length:    10 - 1 = 9 bytes
  #
  #   3. Slice next 9 bytes from the bitstream:
  #      0110 1011 -> 107 -> "k"
  #      0110 0001 ->  97 -> "a"
  #      0110 0110 -> 102 -> "f"
  #      0110 1011 -> 107 -> "k"
  #      0110 0001 ->  97 -> "a"
  #      0010 1101 ->  45 -> "-"
  #      0110 0011 ->  99 -> "c"
  #      0110 1100 -> 108 -> "l"
  #      0110 1001 -> 105 -> "i"
  #
  #   RESULT 1:
  #   client_software_name = "kafka-cli"
  #   rest                 = <<4, 48, 46, 49, 0>>
  #
  # -------------------------------------------------------------------------------------------------------------------------
  # LINE 2: {client_software_version, _rest} = extract_compact_string(rest)
  # -------------------------------------------------------------------------------------------------------------------------
  #
  #   1. Read Next Varint Byte (Length Prefix):
  #      Bits: 0000 0100 (Decimal 4)
  #            ^
  #            |__ MSB is 0. This signals the Varint decoding is complete.
  #
  #   2. Perform Bitwise Reassembly:
  #      - Extract 7 payload bits:  000 0100 -> 4
  #      - Shift and Accumulate:    (4 <<< 0) ||| 0 = 4
  #      - Target String Length:    4 - 1 = 3 bytes
  #
  #   3. Slice next 3 bytes from the bitstream:
  #      0011 0000 -> 48 -> "0"
  #      0011 0100 -> 46 -> "."
  #      0011 0001 -> 49 -> "1"
  #
  #   RESULT 2:
  #   client_software_version = "0.1"
  #   _rest                   = <<0>>  (Leftover trailing tag buffer byte)
  #
  # =========================================================================================================================

  # Classic string: INT16 length prefix, then that many bytes.
  # in:  (2, <<0, 12, "kafka-tester", …>>)
  # out: {"kafka-tester", <<…>>}   (the string, plus the leftover bytes)
  defp extract_string(2, <<length::16, content::binary-size(length), rest::binary>>),
    do: {content, rest}

  # COMPACT_STRING: length prefix is an UNSIGNED_VARINT holding (N + 1).
  #
  # in:  <<10, "kafka-cli", 4, "0.1", 0>>
  # out: {"kafka-cli", <<4, "0.1", 0>>}
  defp extract_compact_string(bytes) do
    IO.inspect(bytes)
    IO.inspect(bytes, binaries: :as_strings)
    IO.inspect(bytes, binaries: :as_binaries)
    # Peel the varint length off the front:  {10, <<"kafka-cli", 4, "0.1", 0>>}
    {len_plus_one, rest} = decode_unsigned_varint(bytes)
    # COMPACT_STRING stores N+1, so subtract 1 for the real byte count:  10 - 1 = 9
    length = len_plus_one - 1
    # Slice `length` bytes off the front:  content = "kafka-cli", rest = <<4, "0.1", 0>>
    <<content::binary-size(length), rest::binary>> = rest
    {content, rest}
  end

  # UNSIGNED_VARINT: 7 payload bits per byte, little-endian, high bit = "more bytes".
  # Walks the bytes in 7-bit steps + the flag bit = 8 bits = 1 byte; when the flag bit is 0 it returns the acc.
  #
  # Entry point: start the running total `acc` at 0 and the `shift` (bit height) at 0.
  # in:  <<10, "kafka-cli", …>>   out: {10, <<"kafka-cli", …>>}
  defp decode_unsigned_varint(bytes), do: decode_unsigned_varint(bytes, 0, 0)

  # acc ||| chunk <<< shift = drop this byte's 7 bits into the right slot of the running total.
  # matches if flag is 1 (top bit set → another byte follows, so recurse)
  #
  # e.g. decoding 300 = <<172, 2>>:  172 = 1_0101100 → chunk 44
  #   recurse with acc = 0 ||| (44 <<< 0) = 44,  shift = 0 + 7 = 7
  defp decode_unsigned_varint(<<1::1, chunk::7, rest::binary>>, acc, shift) do
    decode_unsigned_varint(rest, acc ||| chunk <<< shift, shift + 7)
  end

  # matches if flag is 0 (top bit clear → last byte, finish and return)
  #
  # e.g. A) kafka-cli length: (<<10, "kafka-cli", …>>, 0, 0) → 10 = 0_0001010 → chunk 10
  #         result = 0 ||| (10 <<< 0) = 10  →  {10, <<"kafka-cli", …>>}
  #      B) final byte of 300: (<<2>>, 44, 7) → 2 = 0_0000010 → chunk 2
  #         result = 44 ||| (2 <<< 7) = 44 ||| 256 = 300  →  {300, <<>>}
  defp decode_unsigned_varint(<<0::1, chunk::7, rest::binary>>, acc, shift) do
    {acc ||| chunk <<< shift, rest}
  end

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
