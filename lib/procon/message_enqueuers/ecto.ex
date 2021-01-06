defmodule Procon.MessagesEnqueuers.Ecto do
  use Bitwise

  def build_message(message_body, event_type, message_metadata) do
    message =
      case event_type do
        :updated ->
          %{
            new: Map.get(message_body, "1"),
            old: Map.get(message_body, "1")
          }

        :created ->
          %{
            new: Map.get(message_body, "1")
          }

        :deleted ->
          %{
            old: Map.get(message_body, "1")
          }
      end

    case message_metadata do
      nil -> message
      _ -> Map.put(message, :metadata, message_metadata)
    end
  end

  def build_event_message_versions(event_data, event_type, resource_serializer) do
    Enum.reduce(
      resource_serializer.message_versions,
      %{},
      fn version, versioned_map ->
        Map.put(
          versioned_map,
          version,
          apply(resource_serializer, event_type, [event_data, version])
        )
      end
    )
  end

  def select_partition(partition_key, nb_partitions) when is_binary(partition_key) do
    to_charlist(partition_key)
    |> Enum.reduce(0, fn charcode, hash -> (hash <<< 5) - hash + charcode end)
    |> abs()
    |> rem(nb_partitions)
  end

  def enqueue_rtevent(event_data, event_serializer, options \\ []) do
    key =
      [
        event_serializer.repo |> to_string(),
        Map.get(event_data, :channel, Map.get(event_data, :session_id, "")),
        event_data.event
      ]
      |> IO.iodata_to_binary()

    last_update_time =
      :ets.lookup(:procon_enqueuers_thresholds, key)
      |> case do
        [{_, update_time}] ->
          update_time

        [] ->
          :os.system_time(:millisecond) - event_serializer.threshold - 1
      end

    new_update_time = :os.system_time(:millisecond)

    case new_update_time - last_update_time > event_serializer.threshold do
      true ->
        :ets.insert(:procon_enqueuers_thresholds, {key, new_update_time})
        enqueue_event(event_data, event_serializer, :created, options)

      false ->
        {:ok, :no_enqueue}
    end
  end

  def enqueue_event(event_data, event_serializer, event_type, options \\ []) do
    Logger.metadata(procon_processor_repo: event_serializer.repo)

    message_body = build_event_message_versions(event_data, event_type, event_serializer)

    Keyword.get(options, :topic, event_serializer.topic)
    |> Procon.KafkaMetadata.nb_partitions_for_topic()
    |> case do
      {:error, :unknown_topic, topic} ->
        {:error, :unknown_topic, topic}

      {:ok, nb_partitions} ->
        message_metadata = Keyword.get(options, :metadata)

        message_body
        |> build_message(event_type, message_metadata)
        |> Jason.encode()
        |> case do
          {:ok, message_blob} ->
            enqueue(
              message_blob,
              event_serializer.build_partition_key(event_data)
              |> select_partition(nb_partitions),
              Keyword.get(options, :topic, event_serializer.topic),
              event_serializer.repo
            )

          {:error, error} ->
            {:error, error}
        end
    end
  end

  def enqueue(blob, partition, topic, _repo) do
    :brod.start_producer(:brod_client, topic, [])
    :brod.produce_sync(:brod_client, topic, partition, "", blob)
  end
end
