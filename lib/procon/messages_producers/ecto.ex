defmodule Procon.MessagesProducers.Ecto do
  alias Procon.Models.Ecto.ProconProducerMessage, as: ProducerMessage
  require Logger
  import Ecto.Query

  @spec next_messages_to_send(String.t, integer, integer) :: list(tuple)
  def next_messages_to_send(topic, partition, number_of_messages) do
    try do
      Application.get_env([:procon, :messages_repository])
        .all(from pm in ProducerMessage, 
             where: pm.topic == ^topic 
                    and pm.partition == ^partition,
             limit: ^number_of_messages,
             select: {pm.id, pm.blob},
             order_by: pm.id)
        rescue
          e ->
            Logger.warn("Procon.MessagesProduers.Ecto : exception #{inspect e.message}. Return empty list of messages to produce.")
            :timer.sleep(1000) # to prevent DB spamming in case of troubles
            []
        end
  end

  def delete_rows(ids) do
    q = from pm in ProducerMessage, where: pm.id in ^ids
    case Application.get_env([:procon, :messages_repository]).delete_all(q) do
      {:ok, _} -> {:ok, :next}
      {:error, error} -> {:stop, error}
    end

  end

  @spec log_warning(String.t) :: no_return
  def log_warning(message), do: Logger.warn(message)
end
