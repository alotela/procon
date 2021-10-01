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

    Logger.notice(
      "PROCON : Starting WalDispatcherProducer for topic/partition #{state.topic}/#{state.partition_index}"
    )

    GenServer.cast(self(), :start_producing)

    Process.put(:message_number, 0)
    Process.put(:timestamp_in_ms, 0)

    {:ok, state}
  end

  def handle_cast(:start_producing, state) do
    :ets.match(state.ets_messages_queue_ref, {:_, state.ets_key, :"$1", false})
    |> case do
      [] ->
        # sleep to prevent too fast loop when no messages to send
        Process.sleep(1)
        GenServer.cast(self(), :start_producing)
        nil

      messages ->
        Logger.debug("PROCON > #{__MODULE__} > handle_cast(:start_producing) > received messages",
          metadata: [state: state]
        )

        Logger.debug(messages, metadata: [state: state])

        :brod.produce_sync(
          state.broker_client_name,
          state.topic,
          state.partition_index,
          "",
          Enum.map(messages, &build_message(&1, state))
        )
        |> case do
          :ok ->
            Procon.Parallel.pmap(messages, fn [message] ->
              true = :ets.update_element(state.ets_messages_queue_ref, message.end_lsn, {4, true})
            end)

            GenServer.cast(self(), :start_producing)

          {
            :error,
            {
              :producer_down,
              _details
            }
          } ->
            Logger.warning("producer down, on stoppe....", ansi_color: :red)
            nil
        end
    end

    {:noreply, state}
  end

  def build_message([message], state) do
    timestamp_in_ms = div(message.timestamp, 1000)
    # Process.put(_,_) replace and return the PREVIOUS stored value
    last_timestamp_in_ms = Process.put(:timestamp_in_ms, timestamp_in_ms)

    key =
      Map.get(message.payload, :after)
      |> case do
        nil ->
          Map.get(message.payload, :before)

        payload ->
          payload
      end
      |> Map.get(state.pkey_column)

    monothonic_sequence =
      case timestamp_in_ms == last_timestamp_in_ms do
        true ->
          Process.get(:message_number) + 1

        false ->
          0
      end

    Process.put(:message_number, monothonic_sequence)

    payload =
      Map.get(message, :payload)
      |> Map.put(
        :ts_ms,
        timestamp_in_ms
        # <<
        #  timestamp_in_ms::unsigned-size(48),
        #  monothonic_sequence::unsigned-integer-size(16),
        #  Application.get_env(:procon, :instance_num)::unsigned-integer-size(8),
        #  # 72_057_594_037_927_936 = 2^56
        #  :rand.uniform(72_057_594_037_927_936)::unsigned-integer-size(56)
        # >>
        # |> Ecto.ULID.load()
        # |> elem(1)
      )
      |> Map.put(
        :transaction,
        %{
          id: timestamp_in_ms |> Integer.to_string()
        }
      )

    Procon.MessagesProducers.Kafka.build_message(
      key,
      payload,
      state.avro_value_schema_name,
      state.serialization
    )
  end
end
