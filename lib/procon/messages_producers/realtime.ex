defmodule Procon.MessagesProducers.Realtime do
  @procon_realtime_topic_name "procon-realtime"

  def realtime_topic(), do: @procon_realtime_topic_name

  def send_rtevent(%Procon.Schemas.ProconRealtime{} = event_data, repository, threshold \\ 1000) do
    key = {
      repository,
      Map.get(event_data, :channel, Map.get(event_data, :session_id, ""))
    }

    last_sent_time =
      :ets.lookup(:procon_enqueuers_thresholds, key)
      |> case do
        [{_, sent_time}] ->
          sent_time

        [] ->
          :os.system_time(:millisecond) - threshold - 1
      end

    new_sent_time = :os.system_time(:millisecond)

    case new_sent_time - last_sent_time > threshold do
      true ->
        :ets.insert(:procon_enqueuers_thresholds, {key, new_sent_time})

        spawn(fn ->
          send_to_broker(repository, event_data)
        end)

      false ->
        programmed_sent_time = last_sent_time + threshold

        :ets.insert(:procon_enqueuers_thresholds, {key, programmed_sent_time})

        spawn(fn ->
          Process.sleep(programmed_sent_time - new_sent_time)
          send_to_broker(repository, event_data)
        end)
    end
  end

  def send_to_broker(datastore, payload) do
    :brod.produce_sync(
      Procon.MessagesProducers.WalDispatcher.broker_client_name(%{datastore: datastore}),
      @procon_realtime_topic_name,
      0,
      "",
      [
        build_message(payload, %{
          avro_value_schema_name: @procon_realtime_topic_name,
          serialization: :avro
        })
      ]
    )
  end

  def build_message(message, options) do
    timestamp_in_ms = :os.system_time(:millisecond) |> div(1000)

    payload = %{
      after: message,
      transaction: %{id: timestamp_in_ms}
    }

    key = Map.get(message, :channel, Map.get(message, :session_id, ""))

    Procon.MessagesProducers.Kafka.build_message(
      key,
      payload,
      Procon.Avro.ConfluentSchemaRegistry.topic_to_avro_value_schema(@procon_realtime_topic_name),
      options.serialization,
      %{}
    )
  end
end
