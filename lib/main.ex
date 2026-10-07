defmodule Broker do
  use Application

  def start(_type, _args) do
    Supervisor.start_link([{Task, fn -> Client.listen() end}], strategy: :one_for_one)
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
