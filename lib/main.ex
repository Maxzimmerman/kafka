defmodule Broker do
  use Application

  def start(_type, _args) do
    Supervisor.start_link([{Task, fn -> Broker.listen() end}], strategy: :one_for_one)
  end

  def listen() do
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    IO.puts(:stderr, "Logs from your program will appear here!")

    {:ok, socket} = :gen_tcp.listen(9092, [:binary, active: false, reuseaddr: true])
    listen_loop(socket)
  end

  defp listen_loop(socket) do
    case :gen_tcp.accept(socket) do
      {:ok, client} -> client_loop(client)
    end

    listen_loop(socket)
  end

  defp client_loop(client) do
    case :gen_tcp.recv(client, 0) do
      {:ok, data} -> :gen_tcp.send(client, data)
    end

    client_loop(client)
  end
end

defmodule CLI do
  def main(_args) do
    # Start the Broker application
    {:ok, _pid} = Application.ensure_all_started(:codecrafters_kafka)

    # Run forever
    Process.sleep(:infinity)
  end
end
