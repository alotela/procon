defmodule Procon.MessagesProducers.ProducersStarter do
  alias Procon.MessagesProducers.ProducerGenServer, as: MPPG
  alias Procon.Schemas.Ecto.ProconProducerMessage
  import Ecto.Query

  def start_topic_production(nb_messages, processor_repo, topic) do
    Procon.KafkaMetadata.partition_ids_for_topic(topic)
    |> Enum.each(&MPPG.start_partition_production(&1, nb_messages, processor_repo, topic))
  end

  def start_topics_production_from_database_messages() do
    consumers_datastores()
    |> Enum.each(&start_processor_topics_production_from_database_messages/1)
  end

  def consumers_datastores() do
    Procon.MessagesControllers.ConsumersStarter.activated_consumers()
    |> Enum.map(& &1.datastore)
  end

  def start_processor_topics_production_from_database_messages(
        processor_repo,
        nb_messages \\ Application.get_env(:procon, :nb_simultaneous_messages_to_send)
      ) do
    from(pm in ProconProducerMessage, group_by: pm.topic, select: {pm.topic, count(pm.id)})
    |> processor_repo.all()
    |> Enum.each(fn {topic, _} -> start_topic_production(nb_messages, processor_repo, topic) end)
  end
end
