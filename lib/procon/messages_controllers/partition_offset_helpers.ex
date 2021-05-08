defmodule Procon.PartitionOffsetHelpers do
  def build_ets_key(topic, partition), do: "#{topic}_#{partition}"

  def update_partition_offset_ets(
        processor_name,
        topic,
        partition,
        offset,
        _processing_id \\ "toto"
      ) do
    :ets.insert(
      processor_name,
      {build_ets_key(topic, partition), offset}
    )
  end

  def get_last_processed_offset(repo, topic, partition, processor_name, _processing_id) do
    ets_key = build_ets_key(topic, partition)

    case :ets.lookup(processor_name, ets_key) do
      [] ->
        Ecto.Adapters.SQL.query(
          repo,
          "SELECT message_id FROM procon_consumer_indexes
           WHERE topic = $1
           AND partition = $2",
          [topic, partition]
        )
        |> case do
          {:ok, %Postgrex.Result{num_rows: 1, rows: [[offset]]}} ->
            offset

          {:ok, %Postgrex.Result{num_rows: 0}} ->
            {:ok, %Postgrex.Result{num_rows: 1}} =
              Ecto.Adapters.SQL.query(
                repo,
                "INSERT INTO procon_consumer_indexes (message_id, topic, partition, inserted_at, updated_at) VALUES (-1, $1, $2, NOW(), NOW())",
                [topic, partition]
              )

            -1

          {:error, %DBConnection.ConnectionError{} = err} ->
            Procon.Helpers.inspect(
              err,
              "PROCON FATAL ERROR : unable to get partition offset in DB : #{processor_name}/#{
                topic
              }/#{partition}."
            )

            {:error, err}

          {:error, err} ->
            Procon.Helpers.inspect(
              err,
              "PROCON FATAL ERROR : unable to update partition offset in DB : #{processor_name}/#{
                topic
              }/#{partition}."
            )

            {:error, err}
        end

      [{^ets_key, offset}] ->
        offset
    end
  end

  def update_partition_offset(topic, partition, offset, repo, processor_name) do
    Ecto.Adapters.SQL.query(
      repo,
      "UPDATE procon_consumer_indexes
       SET message_id = $1
       WHERE topic = $2
       AND partition = $3",
      [offset, topic, partition]
    )
    |> case do
      {:ok, %Postgrex.Result{num_rows: 1}} ->
        :ok

      {:error, %DBConnection.ConnectionError{} = err} ->
        Procon.Helpers.inspect(
          err,
          "PROCON FATAL ERROR : unable to update partition offset : #{processor_name}/#{topic}/#{
            partition
          }/#{offset}."
        )

        {:error, err}

      {:error, err} ->
        Procon.Helpers.inspect(
          err,
          "PROCON FATAL ERROR : unable to update partition offset : #{processor_name}/#{topic}/#{
            partition
          }/#{offset}."
        )

        {:error, err}
    end
  end

  def build_ets_key(consumer_message_index) do
    "#{consumer_message_index.topic}_#{consumer_message_index.partition}"
  end
end
