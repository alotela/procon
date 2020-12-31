defmodule Procon.MessagesProducers.WalDispatcherProducer do
  use GenServer
  require Logger

  @spec register_name(any, any, any) :: atom
  def register_name(datastore, topic, partition),
    do: :"wal_dispatcher_producer_#{datastore}_#{topic}_#{partition}"

  def start_link(state) do
    GenServer.start_link(__MODULE__, state, name: state.name)
  end

  def init(state) do
    Logger.metadata(procon_wal_dispatcher_producer: state.name)

    GenServer.cast(self(), :start_producing)

    {:ok, state}
  end

  def handle_cast(:start_producing, state) do
    :ets.match(state.ets_messages_queue_ref, {:_, state.ets_key, :"$1", false})
    |> case do
      [] ->
        nil

      messages ->
        :ok =
          :brod.produce_sync(
            state.broker_client_name,
            state.topic |> Atom.to_string(),
            state.partition_index,
            "",
            messages
            |> Enum.map(fn [message] -> {"", Jason.encode!(message)} end)
          )

        Procon.Parallel.pmap(messages, fn [message] ->
          true = :ets.update_element(state.ets_messages_queue_ref, message.end_lsn, {4, true})
        end)
    end

    GenServer.cast(self(), :start_producing)

    {:noreply, state}
  end

  def build_message(message_body, event_type, message_metadata) do
    message = %{
      body: message_body,
      event: event_type |> to_string()
    }

    case message_metadata do
      nil -> message
      _ -> Map.put(message, :metadata, message_metadata)
    end
  end
end
