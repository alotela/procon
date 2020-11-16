defmodule Procon.MessagesProducers.ProducerLastIndex do
  use GenServer

  def start_link(default) do
    GenServer.start_link(__MODULE__, default, name: __MODULE__)
  end

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_call({:create_sequence, repo, topic, partition}, _from, state) do
    Ecto.Adapters.SQL.query(
      repo,
      "CREATE SEQUENCE IF NOT EXISTS #{get_sequence_name(topic, partition)} MINVALUE 0 NO MAXVALUE START WITH 0 NO CYCLE",
      []
    )
    |> case do
      {:ok, %Postgrex.Result{}} ->
        {:reply, :ok, state}

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

        {:reply, :error, state}
    end
  end

  def create_table() do
    case :ets.whereis(:procon_producer_last_index) do
      :undefined ->
        :procon_producer_last_index =
          :ets.new(:procon_producer_last_index, [:named_table, :public, :set])

      _ ->
        nil
    end
  end

  def get_last_produced_index(repo, topic, partition) do
    create_table()

    :ets.lookup(:procon_producer_last_index, {repo, topic, partition})
    |> case do
      [{{^repo, ^topic, ^partition}, last_id}] ->
        last_id

      _ ->
        do_create_sequence(repo, topic, partition)
        |> case do
          :ok ->
            {:ok, %Postgrex.Result{rows: [[last_id]]}} =
              Ecto.Adapters.SQL.query(
                repo,
                "SELECT last_value FROM #{get_sequence_name(topic, partition)}",
                []
              )

            :ets.insert(:procon_producer_last_index, {{repo, topic, partition}, last_id})

            last_id

          :error ->
            :error
        end
    end
  end

  def set_last_produced_index(repo, topic, partition, last_id) do
    create_table()

    if :ets.lookup(:procon_producer_last_index, {repo, topic, partition}) == [] do
      do_create_sequence(repo, topic, partition)
    else
      :ok
    end
    |> case do
      :ok ->
        Ecto.Adapters.SQL.query(
          repo,
          "SELECT setval('#{get_sequence_name(topic, partition)}', $1, false)",
          [last_id]
        )

        :ets.insert(:procon_producer_last_index, {{repo, topic, partition}, last_id})

        {:ok, last_id}

      :error ->
        :error
    end
  end

  def do_create_sequence(repo, topic, partition) do
    GenServer.call(__MODULE__, {:create_sequence, repo, topic, partition}, :infinity)
  end

  def get_sequence_name(topic, partition),
    do:
      "ppm_lidx_#{to_string(partition)}_#{
        topic |> String.split(["-", "_"]) |> Enum.reverse() |> Enum.join("")
      }_seq"
end
