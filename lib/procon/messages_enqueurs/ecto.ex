defmodule Procon.MessagesEnqueuers.Ecto do
  alias Procon.Schemas.Ecto.ProconProducerMessage
  use Bitwise

  def build_message(message_body, message_event, message_metadata) do
    message = %{
      body: message_body,
      event: message_event,
      # this is a fake index replaced by
      # real index when we put messages in kafka
      # since it is autoincremented by the datastore
      # when we store the message in database
      index: 1
    }

    case message_metadata do
      nil -> message
      _ -> Map.put(message, :metadata, message_metadata)
    end
  end

  @spec build_event_message_versions(map, :atom, module) :: map()
  def build_event_message_versions(event_data, event_status, resource_serializer) do
    Enum.reduce(
      resource_serializer.message_versions, 
      %{}, 
      fn(version, versioned_map) ->
        Map.put(versioned_map, version, apply(resource_serializer, event_status, [event_data, version]))
      end
    )
  end

  def select_partition(partition_key, nb_partitions) when is_binary(partition_key) do
    to_charlist(partition_key)
    |> Enum.reduce(0, fn(charcode, hash) -> (hash <<< 5) - hash + charcode end)
    |> abs()
    |> rem(nb_partitions)
  end

  @spec enqueue_rtevent(map, list) :: {:ok, Ecto.Schema.t} | {:error, Ecto.Changeset.t} | {:error, term}
  def enqueue_rtevent(event_data, options \\ []) do
    service_name = Application.get_env(:procon, :service_name) || Keyword.get(options, :service_name)
    topic = Keyword.get(options, :topic) || Application.get_env(:procon, :default_realtime_topic) 
    message_event = "#{Application.get_env(:procon, :service_name)}/real_time/notify"
    message_metadata = Keyword.get(options, :metadata)
    message_body = %{
      1 => %{payload: %{ channel: event_data.channel, source: service_name } }
    }
    partition_key = Keyword.get(options, :pkey) || service_name
    message_partition = select_partition(partition_key, Procon.KafkaMetadata.nb_partitions_for_topic(topic))

    case message_body |> build_message(message_event, message_metadata) |> Poison.encode() do
      {:ok, message_blob} -> enqueue(message_blob, message_partition, topic)
      {:error, error} -> {:error, error}
    end
  end

  @spec enqueue_event(map, module, :atom, list) :: {:ok, Ecto.Schema.t} | {:error, Ecto.Changeset.t} | {:error, term}
  def enqueue_event(event_data, event_serializer, event_status, options \\ []) do
    message_event = event_serializer.build_message_event(event_status)
    message_body = build_event_message_versions(event_data, event_status, event_serializer)
    message_partition = event_serializer.build_partition_key(event_data)
                        |> select_partition(
                          Procon.KafkaMetadata.nb_partitions_for_topic(event_serializer.topic)
                        )
    message_metadata = Keyword.get(options, :metadata)

    case message_body |> build_message(message_event, message_metadata) |> Poison.encode() do
      {:ok, message_blob} -> enqueue(message_blob, message_partition, event_serializer.topic)
      {:error, error} -> {:error, error}
    end
  end

  @spec enqueue(String.t, String.t, String.t) :: {:ok, Ecto.Schema.t} | {:error, Ecto.Changeset.t} | {:error, term}
  def enqueue(blob, partition, topic) do
    Application
    .get_env(:procon, :messages_repository)
    .insert(%ProconProducerMessage{
      topic: topic, 
      partition: partition, 
      blob: blob
    })
  end
end
