defmodule Client do
  @moduledoc """
  module that completely handles the tcp request response and client handlind
  """

  @doc "fires the whole tcp request response loop"
  def listen() do
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    IO.puts(:stderr, "Logs from your program will appear here!")

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
      {:ok, data} -> :gen_tcp.send(client, WireProtocol.response(data))
    end

    client_loop(client)
  end
end
