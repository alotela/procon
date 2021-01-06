defmodule Procon.MessagesProducers.ProducersStarter do
  alias Procon.MessagesProducers.ProducerGenServer, as: MPPG
  alias Procon.Schemas.Ecto.ProconProducerMessage
  import Ecto.Query

  def start_topic_production(module, nb_messages \\ 1000),
    do: start_topic_production(nb_messages, module.repo(), module.topic())

  def start_topic_production(nb_messages, processor_repo, topic) do
    case Procon.KafkaMetadata.partition_ids_for_topic(topic) do
      nil ->
        throw("The topic #{topic} does not seem to exists. Program")

      partition_ids ->
        Enum.each(
          partition_ids,
          &MPPG.start_partition_production(&1, nb_messages, processor_repo, topic)
        )
    end
  end

  def start_topics_production_from_database_messages() do
    consumers_datastores()
    |> Enum.each(&start_processor_topics_production_from_database_messages/1)
  end

  def consumers_datastores() do
    Procon.MessagesControllers.ConsumersStarter.activated_consumers_configs()
    |> Enum.map(& &1.datastore)
  end

  def start_processor_topics_production_from_database_messages(
        processor_repo,
        nb_messages \\ Application.get_env(:procon, :nb_simultaneous_messages_to_send)
      ) do
    Logger.metadata(procon_processor_repo: processor_repo)
    from(pm in ProconProducerMessage,
      group_by: pm.topic_partition,
      select: fragment("REGEXP_REPLACE(topic_partition::varchar,'_[^_]*$','')")
    )
    |> processor_repo.all()
    |> Enum.each(fn {topic, _} -> start_topic_production(nb_messages, processor_repo, topic) end)
  end

  def start_producer({repo, topic}) do
    topic
    |> Procon.KafkaMetadata.partition_ids_for_topic()
    |> case do
      nil ->
        throw("The topic #{topic} does not seem to exists. Program")

      partition_ids ->
        Enum.each(
          partition_ids,
          &MPPG.start_producer(repo, topic, &1)
        )
    end
  end

  def start_activated_processors() do
    activated_producers()
    |> Procon.Parallel.pmap(&start_producer/1)
  end

  def activated_producers() do
    Application.get_env(:procon, Processors)
    |> Enum.filter(
      &Enum.member?(Application.get_env(:procon, :activated_processors), elem(&1, 0))
    )
    |> Enum.reduce([], &(get_producer_info(Keyword.get(elem(&1, 1), :producers, %{})) ++ &2))
  end

  def get_producer_info(%{datastore: repo, topics: topics}) when is_list(topics),
    do: topics |> Enum.map(fn t -> {repo, t} end)

  def get_producer_info(%{datastore: repo, topics: topic}) when is_binary(topic),
    do: [repo, topic]

  def get_producer_info(%{}), do: []
end
