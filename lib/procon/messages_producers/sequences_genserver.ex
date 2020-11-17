defmodule Procon.MessagesProducers.SequencesGenServer do
  use GenServer

  def start_link(default) do
    GenServer.start_link(__MODULE__, default, name: __MODULE__)
  end

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_call({:create_sequence, repo, sequence_name, options}, _from, state) do
    res =
      Ecto.Adapters.SQL.query(
        repo,
        "CREATE SEQUENCE IF NOT EXISTS #{sequence_name} #{options}",
        []
      )

    {:reply, res, state}
  end

  def create_sequence(repo, sequence_name, options) do
    GenServer.call(__MODULE__, {:create_sequence, repo, sequence_name, options}, :infinity)
  end
end
