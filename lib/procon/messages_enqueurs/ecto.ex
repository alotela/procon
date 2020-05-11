defmodule Procon.MessagesEnqueuers.Ecto do
  alias Procon.Schemas.Ecto.ProconProducerMessage
  use Bitwise

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

  @spec build_event_message_versions(map, :atom, module) :: map()
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

  @spec enqueue_rtevent(map, Ecto.Repo.t(), list) ::
          {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, term}
  def enqueue_rtevent(event_data, repo, options \\ []) do
    service_name =
      Keyword.get(options, :service_name) || Application.get_env(:procon, :service_name)

    topic = Keyword.get(options, :topic) || Application.get_env(:procon, :default_realtime_topic)
    message_event = "#{Application.get_env(:procon, :service_name)}/real_time/notify"
    message_metadata = Keyword.get(options, :metadata)

    message_body = %{
      1 => %{payload: %{channel: event_data.channel, source: service_name}}
    }

    partition_key = Keyword.get(options, :pkey) || service_name

    message_partition =
      select_partition(partition_key, Procon.KafkaMetadata.nb_partitions_for_topic(topic))

    case message_body |> build_message(message_event, message_metadata) |> Jason.encode() do
      {:ok, message_blob} -> enqueue(message_blob, message_partition, topic, repo)
      {:error, error} -> {:error, error}
    end
  end

  @spec enqueue_event(map, module, :atom, list) ::
          {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, term}
  def enqueue_event(event_data, event_serializer, event_type, options \\ []) do
    Logger.metadata(procon_processor_repo: event_serializer.repo)
    message_body = build_event_message_versions(event_data, event_type, event_serializer)

    message_partition =
      event_serializer.build_partition_key(event_data)
      |> select_partition(
        Procon.KafkaMetadata.nb_partitions_for_topic(event_serializer.topic)
        |> elem(1)
      )

    message_metadata = Keyword.get(options, :metadata)

    case message_body |> build_message(event_type, message_metadata) |> Jason.encode() do
      {:ok, message_blob} ->
        enqueue(message_blob, message_partition, event_serializer.topic, event_serializer.repo)

      {:error, error} ->
        {:error, error}
    end
  end

  @spec enqueue(String.t(), String.t(), String.t(), Ecto.Repo.t()) ::
          {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, term}
  def enqueue(blob, partition, topic, repo) do
    repo.insert(%ProconProducerMessage{
      topic: topic,
      partition: partition,
      blob: blob
    })
  end
end
