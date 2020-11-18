defmodule Procon.MessagesProducers.ProducerSequences do
  def create_table() do
    case :ets.whereis(:procon_producer_sequence) do
      :undefined ->
        try do
          :procon_producer_sequence =
            :ets.new(:procon_producer_sequence, [:named_table, :public, :set])
        rescue
          _e in ArgumentError -> :procon_producer_sequence
        end

      _ ->
        nil
    end
  end

  def has_sequence(repo, topic, partition) do
    create_table()
    :ets.lookup(:procon_producer_sequence, {repo, topic, partition}) != []
  end

  def create_sequence(repo, topic, partition, force) do
    if not has_sequence(repo, topic, partition) || force do
      do_create_sequence(repo, topic, partition)
    end
  end

  def do_create_sequence(repo, topic, partition) do
    Procon.MessagesProducers.SequencesGenServer.create_sequence(
      repo,
      get_sequence_name(topic, partition),
      "INCREMENT BY 1 MINVALUE 1 NO MAXVALUE START WITH 1 NO CYCLE"
    )
    |> case do
      {:ok, %Postgrex.Result{}} ->
        :ets.insert(:procon_producer_sequence, {{repo, topic, partition}})
        :ok

      err ->
        IO.inspect(err,
          label:
            "PROCON ALERT : DB error in index sequence creation for repo #{inspect(repo)}, partition #{
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
