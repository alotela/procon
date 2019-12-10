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
  def cache_kafka_metadata do
    :procon = :ets.new(:procon, [:named_table, :set])

    {:ok, %{brokers: _, cluster_id: _, controller_id: _, topic_metadata: topic_metadata}} =
      :brod.get_metadata(Application.get_env(:procon, :brokers))

    true =
      :ets.insert(
        :procon,
        {
          :kafka_topics,
          extract_topics_metadata(topic_metadata)
        }
      )
  end

  def kafka_topics_metadata do
    :ets.lookup_element(:procon, :kafka_topics, 2)
  end

  def partition_ids_for_topic(topic) do
    kafka_topics_metadata() |> Map.get(topic)
  end

  @spec nb_partitions_for_topic(String.t()) :: {atom, integer}
  def nb_partitions_for_topic(topic) do
    kafka_topics_metadata()
    |> Map.get(topic)
    |> case do
      nil ->
        IO.inspect("topic #{topic} not found for partition nb")
        {:error, 0}

      topic_partitions ->
        {:ok, topic_partitions |> Enum.count()}
    end
  end

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
      topic_metadata.topic,
      topic_metadata.partition_metadata
      |> Enum.reduce([], &[&1.partition | &2])
    )
  end
end
