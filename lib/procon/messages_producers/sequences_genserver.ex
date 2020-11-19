defmodule Procon.MessagesProducers.SequencesGenServer do
  use GenServer

  def via(process_name), do: {:via, Registry, {Procon.SequencesRegistry, process_name}}

  def start_link(state) do
    GenServer.start_link(__MODULE__, state, name: state.process_name)
  end

  @impl true
  def init(state) do
    IO.inspect(state.repo, label: "PROCON - creating sequence server")
    {:ok, state}
  end

  def start_repo_genserver(processor_repo) do
    DynamicSupervisor.start_child(
      Procon.MessagesProducers.SequencesSupervisor,
      %{
        start:
          {__MODULE__, :start_link,
           [
             %{process_name: process_name(processor_repo), repo: processor_repo}
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
    Application.get_env(:procon, Processors)
    |> Enum.filter(
      &Enum.member?(Application.get_env(:procon, :activated_processors), elem(&1, 0))
    )
    |> Enum.reduce([], &(get_producer_info(Keyword.get(elem(&1, 1), :producers, %{})) ++ &2))
  end

  def get_producer_info(%{datastore: repo}), do: [repo]

  def get_producer_info(%{}), do: []

  @impl true
  def handle_call({:create_sequence, repo, sequence_name, options}, _from, state) do
    res =
      Ecto.Adapters.SQL.query(
        repo,
        "CREATE SEQUENCE IF NOT EXISTS #{sequence_name} #{options}",
        []
      )
      |> IO.inspect(
        label: "PROCON : create_sequence : creating sequence #{sequence_name} for repo #{repo}"
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
