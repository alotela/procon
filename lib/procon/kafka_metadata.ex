defmodule Procon.KafkaMetadata do
  @doc """
  brod.get_metadata returns this structure:
  {
  :ok,
  %{
    brokers: [%{host: "192.168.0.12", node_id: 0, port: 9092, rack: ""}],
    cluster_id: "ym8qVGPwSZCu4idgKzCIyg",
    controller_id: 0,
    topic_metadata: [
      %{
        error_code: :no_error,
        is_internal: false,
        partition_metadata: [
          %{
            error_code: :no_error,
            isr: [0],
            leader: 0,
            partition: 0,
            replicas: [0]
          },
          ...
        ],
        topic: "demo"
      },
    ]
  }
  }
  """
  def cache_kafka_metadata() do
    case :ets.whereis(:procon) do
      :undefined ->
        :procon = :ets.new(:procon, [:named_table, :public, :set])

      _ ->
        nil
    end

    topic_metadata =
      case :brod.get_metadata(Application.get_env(:procon, :brokers)) do
        {:ok, %{brokers: _, cluster_id: _, controller_id: _, topic_metadata: topic_metadata}} ->
          topic_metadata

        {:ok, %{brokers: _, cluster_id: _, controller_id: _, topics: topic_metadata}} ->
          topic_metadata
      end

    true =
      :ets.insert(
        :procon,
        {
          :kafka_topics,
          extract_topics_metadata(topic_metadata)
        }
      )
  end

  def kafka_topics_metadata() do
    :ets.lookup_element(:procon, :kafka_topics, 2)
  end

  def partition_ids_for_topic(topic) do
    kafka_topics_metadata() |> Map.get(topic)
  end

  @spec nb_partitions_for_topic(String.t()) ::
          {:ok, integer} | {:error, :unknown_topic, String.t()}
  def nb_partitions_for_topic(topic, force_refresh \\ false) do
    kafka_topics_metadata()
    |> Map.get(topic)
    |> case do
      nil ->
        case force_refresh do
          true ->
            {:error, :unknown_topic, topic}

          false ->
            cache_kafka_metadata()
            nb_partitions_for_topic(topic, true)
        end

      topic_partitions ->
        {:ok, topic_partitions |> Enum.count()}
    end
  end

  def nb_partitions_for_topic!(topic) when is_atom(topic),
    do: topic |> Atom.to_string() |> nb_partitions_for_topic() |> elem(1)

  def nb_partitions_for_topic!(topic),
    do: topic |> nb_partitions_for_topic() |> elem(1)

  @doc """
  returns a map with topics names anb partitions id
  %{
    "topic0" => [0, 1, 2],
    "topic1" => [0, 1],
    ...
  }
  """
  def extract_topics_metadata(topic_metadata) do
    Enum.reduce(topic_metadata, %{}, &extract_topic_partitions/2)
  end

  def extract_topic_partitions(topic_metadata, topics_acc) do
    Map.put(
      topics_acc,
      Map.get(topic_metadata, :topic) || Map.get(topic_metadata, :name),
      Map.get(topic_metadata, :partition_metadata) ||
        Map.get(topic_metadata, :partitions)
        |> Enum.reduce([], &[Map.get(&1, :partition) || Map.get(&1, :partition_index) | &2])
    )
  end
end
