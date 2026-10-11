defmodule Client do
  @moduledoc """
  module that completely handles the tcp request response and client handlind
  """

  alias Kafka.Protocol

  @doc "fires the whole tcp request response loop"
  def listen() do
    {:ok, socket} = :gen_tcp.listen(9092, [:binary, active: false, reuseaddr: true])
    listen_loop(socket)
  end

  defp listen_loop(socket) do
    case :gen_tcp.accept(socket) do
      {:ok, client} ->
        {:ok, pid} = Task.start_link(fn -> client_loop(client) end)
        :ok = :gen_tcp.controlling_process(client, pid)
    end

    listen_loop(socket)
  end

  defp client_loop(client) do
    case :gen_tcp.recv(client, 0) do
      {:ok, data} ->
        request_struct = Protocol.request(data)
        response = Protocol.response(request_struct)
        :gen_tcp.send(client, response)
    end

    client_loop(client)
  end
end
