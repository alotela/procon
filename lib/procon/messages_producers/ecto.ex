defmodule Procon.MessagesProducers.Ecto do
  alias Procon.Schemas.Ecto.ProconProducerMessage
  require Logger
  import Ecto.Query

  @spec next_messages_to_send(String.t(), integer, Atom) :: list(tuple)
  def next_messages_to_send(topic_partition, number_of_messages, processor_repo) do
    try do
      %Postgrex.Result{rows: rows} =
        Ecto.Adapters.SQL.query!(
          processor_repo,
          "SELECT id, replace(blob, '@procon_batch@', $1)
        FROM procon_producer_messages
        WHERE topic_partition=$2
        ORDER BY id ASC
        LIMIT $3",
          [Ecto.ULID.generate(), Atom.to_string(topic_partition), number_of_messages]
        )

      rows
    rescue
      e ->
        Logger.warn(
          "Procon.MessagesProduers.Ecto. Return from exception with empty list of messages to produce."
        )

        IO.inspect(e, label: "exception rescued")
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
          topic_partition: "refresh_events_0",
          blob: "coucou#{n}"
        }
      end

    IO.inspect(DateTime.utc_now(), label: "list")
    processor_repo.insert_all(ProconProducerMessage, records)
    IO.inspect(DateTime.utc_now(), label: "end")
  end
end
