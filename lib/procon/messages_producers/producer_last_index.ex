defmodule Procon.MessagesProducers.ProducerLastIndex do
  def create_table() do
    case :ets.whereis(:procon_producer_last_index) do
      :undefined ->
        try do
          :procon_producer_last_index =
            :ets.new(:procon_producer_last_index, [:named_table, :public, :set])
        rescue
          _e in ArgumentError -> :procon_producer_last_index
        end

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
        create_sequence(repo, topic, partition)
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
      create_sequence(repo, topic, partition)
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

  def init_sequence(repo, topic, partition) do
    get_last_produced_index(repo, topic, partition)
    |> case do
      :error -> :error
      _ -> :ok
    end
  end

  def create_sequence(repo, topic, partition) do
    Procon.MessagesProducers.SequencesGenServer.create_sequence(
      repo,
      get_sequence_name(topic, partition),
      "MINVALUE 0 NO MAXVALUE START WITH 0 NO CYCLE"
    )
    |> case do
      {:ok, %Postgrex.Result{}} ->
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
      "ppm_lidx_#{to_string(partition)}_#{
        topic |> String.split(["-", "_"]) |> Enum.reverse() |> Enum.join("")
      }_seq"
end
