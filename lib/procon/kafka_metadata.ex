defmodule Procon.KafkaMetadata do
  @doc """
  brod.get_metadata returns this structure:
  {:ok,
    [
      brokers: [[node_id: 0, host: "localhost", port: 9092, rack: :undefined]],
      cluster_id: "RT6aIadOThuSWPF5ZfeCqQ",
      controller_id: 0,
      topic_metadata: [
        [
          topic_error_code: :no_error,
          topic: "refresh_events",
          is_internal: false,
          partition_metadata: [
            [
              partition_error_code: :no_error,
              partition_id: 2,
              leader: 0,
              replicas: [0],
              isr: [0]
            ],
            [
              partition_error_code: :no_error,
              partition_id: 1,
              leader: 0,
              replicas: [0],
              isr: [0]
            ],
            [
              partition_error_code: :no_error,
              partition_id: 0,
              leader: 0,
              replicas: [0],
              isr: [0]
            ]
          ]
        ]
      ]
   ]}
  """ 
  def cache_kafka_metadata do
    :procon = :ets.new(:procon, [:named_table, :set])
    {:ok, metadata} = :brod.get_metadata([localhost: 9092])
    true = :ets.insert(
      :procon,
      {
        :kafka_topics,
        extract_topics_metadata(metadata) 
      }
    )
  end

  def kafka_topics_metadata do
    :ets.lookup_element(:procon, :kafka_topics, 2)
  end

  def partition_ids_for_topic(topic) do
    kafka_topics_metadata() |> Map.get(topic)
  end

  @spec nb_partitions_for_topic(String.t) :: integer
  def nb_partitions_for_topic(topic) do
    kafka_topics_metadata() |> Map.get(topic) |> Enum.count()
  end

  @doc """
  returns a map with topics names anb parittions id
  %{
    "topic0" => [0, 1, 2],
    "topic1" => [0, 1],
    ...
  }
  """
  def extract_topics_metadata(kafka_metadata) do
    kafka_metadata
    |> List.keyfind(:topic_metadata, 0) |> elem(1)
    |> Enum.reduce(%{}, &extract_topic_partitions/2)
  end

  def extract_topic_partitions(topic_metadata, topics_acc) do
    Map.put(
      topics_acc,
      List.keyfind(topic_metadata, :topic, 0) |> elem(1),
      List.keyfind(topic_metadata, :partition_metadata, 0)
      |> elem(1)
      |> Enum.reduce(
        [],
        fn(partition_meta, partition_ids) ->
          [ partition_meta |> List.keyfind(:partition_id, 0) |> elem(1) | partition_ids ]
        end
      )
    )
  end
end
