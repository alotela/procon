defmodule Procon.MessagesProducers.Ecto do
  alias Procon.Models.Ecto.ProconProducerMessage, as: ProducerMessage
  require Logger
  import Ecto.Query

  @spec next_messages_to_send(String.t, integer, integer) :: list(tuple)
  def next_messages_to_send(topic, partition, number_of_messages) do
    try do
      Application.get_env(:procon, :messages_repository)
        .all(from pm in ProducerMessage, 
             where: pm.topic == ^topic 
                    and pm.partition == ^partition,
             limit: ^number_of_messages,
             select: [:id, :blob],
             order_by: pm.id)
        rescue
          e ->
            Logger.warn("Procon.MessagesProduers.Ecto. Return empty list of messages to produce.")
            IO.inspect(e)
            :timer.sleep(1000) # to prevent DB spamming in case of troubles
            []
        end
  end

  def delete_rows(ids) do
    q = from pm in ProducerMessage, where: pm.id in ^ids
    case Application.get_env(:procon, :messages_repository).delete_all(q) do
      {_, nil} -> {:ok, :next}
      {:error, error} -> {:stop, error}
    end

  end

  def generate_records() do
    records = for n <- 1..10000 do
      %{
        topic: "refresh_events",
        partition: 0,
        blob: "coucou#{n}",
        inserted_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now()
      }
    end

    Application.get_env(:procon, :messages_repository).insert_all(Procon.Models.Ecto.ProconProducerMessage, records)
  end

end
