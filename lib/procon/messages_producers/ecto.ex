defmodule Procon.MessagesProducers.Ecto do
  alias Procon.Schemas.Ecto.ProconProducerMessage
  require Logger
  import Ecto.Query

  @spec next_messages_to_send(String.t(), integer, integer, Atom) :: list(tuple)
  def next_messages_to_send(topic, partition, number_of_messages, processor_repo) do
    try do
      processor_repo.all(
        from(pm in ProconProducerMessage,
          where:
            pm.topic == ^topic and
              pm.partition == ^partition,
          limit: ^number_of_messages,
          select: {pm.index, {"", pm.blob}},
          order_by: pm.index
        )
      )
    rescue
      e ->
        Logger.warn("Procon.MessagesProduers.Ecto. Return empty list of messages to produce.")
        IO.inspect(e)
        # to prevent DB spamming in case of troubles
        :timer.sleep(1000)
        []
    end
  end

  def delete_rows(processor_repo, topic, partition, ids) do
    q =
      from(pm in ProconProducerMessage,
        where: pm.index in ^ids and pm.partition == ^partition and pm.topic == ^topic
      )

    try do
      case processor_repo.delete_all(q) do
        {_, nil} -> {:ok, :next}
        {:error, error} -> {:stop, error}
      end
    rescue
      e ->
        {:stop, e}
    end
  end

  def generate_records(processor_repo) do
    records =
      for n <- 1..10000 do
        %{
          topic: "refresh_events",
          partition: 0,
          blob: "coucou#{n}",
          inserted_at: DateTime.utc_now(),
          updated_at: DateTime.utc_now()
        }
      end

    IO.inspect(DateTime.utc_now(), label: "list")
    processor_repo.insert_all(ProconProducerMessage, records)
    IO.inspect(DateTime.utc_now(), label: "end")
  end
end
