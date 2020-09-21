defmodule Procon.MessagesEnqueuers.Ecto do
  @type states() :: :created | :updated | :deleted
  alias Procon.Schemas.Ecto.ProconProducerMessage
  use Bitwise

  @spec build_message(map(), states() | String.t(), map()) :: %{
          :body => any,
          :event => binary,
          optional(:metadata) => any
        }
  def build_message(message_body, event_type, message_metadata) do
    message = %{
      body: message_body,
      event: event_type |> to_string()
    }

    case message_metadata do
      nil -> message
      _ -> Map.put(message, :metadata, message_metadata)
    end
  end

  @spec build_event_message_versions(map, states(), module) :: map()
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

  @spec select_partition(binary, integer) :: integer
  def select_partition(partition_key, nb_partitions) when is_binary(partition_key) do
    to_charlist(partition_key)
    |> Enum.reduce(0, fn charcode, hash -> (hash <<< 5) - hash + charcode end)
    |> abs()
    |> rem(nb_partitions)
  end

  @spec enqueue_rtevent(map, Ecto.Repo.t(), list) ::
          {:ok, Ecto.Schema.t()}
          | {:error, Ecto.Changeset.t()}
          | {:error, term}
          | {:ok, :no_enqueue}
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

  @spec enqueue_event(map(), module(), states(), list()) ::
          {:ok, Ecto.Schema.t()}
          | {:error, Ecto.Changeset.t()}
          | {:error, term}
          | {:error, :unknown_topic, String.t()}
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

  @spec enqueue(String.t(), pos_integer(), String.t(), Ecto.Repo.t()) ::
          {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, term}
  def enqueue(blob, partition, topic, repo) do
    repo.insert(%ProconProducerMessage{
      topic: topic,
      partition: partition,
      blob: blob
    })
  end
end
