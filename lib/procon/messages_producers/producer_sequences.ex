defmodule Procon.MessagesProducers.ProducerSequences do
  use GenServer

  def start_link(default) do
    GenServer.start_link(__MODULE__, default, name: __MODULE__)
  end

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_call({:create_sequence, repo, topic, partition, force}, _from, state) do
    if not has_sequence(repo, topic, partition) || force do
      do_create_sequence(repo, topic, partition)
    end

    {:reply, :ok, state}
  end

  def create_table() do
    case :ets.whereis(:procon_producer_sequence) do
      :undefined ->
        :procon_producer_sequence =
          :ets.new(:procon_producer_sequence, [:named_table, :public, :set])

      _ ->
        nil
    end
  end

  def has_sequence(repo, topic, partition) do
    create_table()
    :ets.lookup(:procon_producer_sequence, {repo, topic, partition}) != []
  end

  def create_sequence(repo, topic, partition, force) do
    GenServer.call(__MODULE__, {:create_sequence, repo, topic, partition, force}, :infinity)
  end

  def do_create_sequence(repo, topic, partition) do
    Ecto.Adapters.SQL.query(
      repo,
      "CREATE SEQUENCE IF NOT EXISTS #{get_sequence_name(topic, partition)} INCREMENT BY 1 MINVALUE 1 NO MAXVALUE START WITH 1 NO CYCLE",
      []
    )
    |> case do
      {:ok, %Postgrex.Result{}} ->
        :ets.insert(:procon_producer_sequence, {{repo, topic, partition}})
        :ok

      err ->
        IO.inspect(err,
          label:
            "PROCON ALERT : DB error in last index sequence creation for repo #{inspect(repo)}, partition #{
              to_string(partition)
            } and topic #{topic}",
          syntax_colors: [
            atom: :red,
            binary: :red,
            boolean: :red,
            list: :red,
            map: :red,
            number: :red,
            regex: :red,
            string: :red,
            tuple: :red
          ]
        )

        :error
    end
  end

  def get_sequence_name(topic, partition),
    do:
      "ppm_idx_#{to_string(partition)}_#{
        topic |> String.split(["-", "_"]) |> Enum.reverse() |> Enum.join("")
      }_seq"
end
