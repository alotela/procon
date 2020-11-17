defmodule Procon.MessagesProducers.SequencesGenServer do
  use GenServer

  def via(process_name), do: {:via, Registry, {Procon.SequencesRegistry, process_name}}

  def start_link(state) do
    GenServer.start_link(__MODULE__, state, name: state.process_name)
  end

  @impl true
  def init(state) do
    {:ok, state}
  end

  def start_repo_genserver(processor_repo) do
    DynamicSupervisor.start_child(
      Procon.MessagesProducers.SequencesSupervisor,
      %{
        start:
          {__MODULE__, :start_link,
           [
             %{process_name: process_name(processor_repo)}
           ]},
        id: process_name(processor_repo)
      }
    )
  end

  def process_name(repo), do: :"sequences_#{repo}"

  def start_sequences_genservers() do
    consumers_datastores()
    |> Enum.each(&start_repo_genserver/1)
  end

  def consumers_datastores() do
    Procon.MessagesControllers.ConsumersStarter.activated_consumers()
    |> Enum.map(& &1.datastore)
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
    GenServer.call(
      process_name(repo),
      {:create_sequence, repo, sequence_name, options},
      :infinity
    )
  end
end
