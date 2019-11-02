defmodule Procon.MessagesProducers.ProducersStarter do
  alias Procon.MessagesProducers.ProducerGenServer, as: MPPG
  alias Procon.Schemas.Ecto.ProconProducerMessage
  import Ecto.Query

  def start_topic_production(topic, nb_messages \\ 1000) do
    Procon.KafkaMetadata.partition_ids_for_topic(topic)
    |> Enum.each(&MPPG.start_partition_production(topic, &1, nb_messages))
  end

  def start_topics_production_from_database_messages() do
    app_repository = Application.get_env(:procon, :messages_repository)

    from(pm in ProconProducerMessage, group_by: pm.topic, select: {pm.topic, count(pm.id)})
    |> app_repository.all()
    |> Enum.each(fn {topic, _} -> start_topic_production(topic) end)
  end
end
