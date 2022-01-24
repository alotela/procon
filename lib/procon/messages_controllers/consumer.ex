defmodule Procon.MessagesControllers.Consumer do
  require Record

  Record.defrecord(
    :kafka_message,
    Record.extract(:kafka_message, from_lib: "brod/include/brod.hrl")
  )

  def init(consumer_config, init_data) do
    Logger.metadata(procon_processor: consumer_config.group_id)
    {:ok, Map.merge(consumer_config, init_data)}
  end

  def handle_message({:kafka_message_set, topic, partition, _high_wm_offset, messages}, state) do
    master_processing_id = :rand.uniform(99_999_999) + 100_000_000

    try do
      for {:kafka_message, offset, _key, kafka_message_content, _ts_type, _ts, _headers} <-
            messages do
        processing_id = :rand.uniform(99_999_999) + 100_000_000

        try do
          (offset >
             Procon.PartitionOffsetHelpers.get_last_processed_offset(
               state.processor_consumer_config.datastore,
               topic,
               partition,
               state.ets_table_name,
               processing_id
             ))
          |> case do
            false ->
              {:ok, :message_already_processed}

            true ->
              process_message(
                state,
                kafka_message_content,
                topic,
                partition,
                offset,
                processing_id
              )
          end
          |> case do
            :ok ->
              nil

            {:ok, :message_already_processed} ->
              Procon.Helpers.log([
                kafka_message_content,
                "#{master_processing_id}_#{processing_id}@@PROCON FLOW : message already processed : #{state.processor_consumer_config.name}/#{topic}/#{partition}/#{offset} >>> kafka_message_content"
              ])

            {:error, :process_message, :topic_not_listened} ->
              Procon.Helpers.log([
                kafka_message_content,
                "#{master_processing_id}_#{processing_id}@@PROCON FLOW : topic not listened : #{state.processor_consumer_config.name}/#{topic}/#{partition}/#{offset} >>> kafka_message_content"
              ])

            {:error, :process_message, :socket_closed_remotely} ->
              Procon.Helpers.log([
                kafka_message_content,
                "#{master_processing_id}_#{processing_id}@@PROCON_KAFKA_BROD_SOCKET_CLOSED:#{state.processor_consumer_config.name}/#{topic}/#{partition}/#{offset} >>> kafka_message_content"
              ])

            {:error, :process_message, {:failed_connect, host, port}} ->
              Procon.Helpers.log([
                kafka_message_content,
                "#{master_processing_id}_#{processing_id}@@PROCON_KAFKA_BROD_FAILED_CONNECT:#{state.processor_consumer_config.name}/#{topic}/#{partition}/#{offset}@#{host}:#{port} >>> kafka_message_content"
              ])

            {:error, :unable_to_update_offset_in_ets} ->
              Procon.Helpers.log([
                kafka_message_content,
                "#{master_processing_id}_#{processing_id}@@PROCON FLOW : unable to update offset in ets : #{state.processor_consumer_config.name}/#{topic}/#{partition}/#{offset} >>> kafka_message_content"
              ])

              throw({:stop_procon_brod_client, processing_id, offset})

            {:error, error} ->
              Procon.Helpers.log([
                error,
                "#{master_processing_id}_#{processing_id}@@PROCON FLOW : error from processing : #{state.processor_consumer_config.name}/#{topic}/#{partition}/#{offset} >>> error"
              ])

              throw({:stop_procon_brod_client, processing_id, offset})
          end
        rescue
          e ->
            Procon.Helpers.inspect(
              e,
              "#{master_processing_id}_#{processing_id}@@PROCON EXCEPTION#exception in procon handle_message #{state.processor_consumer_config.name}/#{topic}/#{partition}/#{offset}"
            )

            Procon.Helpers.inspect(
              kafka_message_content,
              "#{master_processing_id}_#{processing_id}@@PROCON EXCEPTION#kafka_message_content (not deserialized) in procon handle_message #{state.processor_consumer_config.name}/#{topic}/#{partition}/#{offset}"
            )

            case Avrora.decode(kafka_message_content) do
              {:error, _} ->
                nil

              deserialized_message ->
                Procon.Helpers.inspect(
                  deserialized_message
                  |> elem(1)
                  |> Procon.Helpers.map_keys_to_atom(%Procon.Types.DebeziumMessage{}),
                  "#{master_processing_id}_#{processing_id}@@PROCON EXCEPTION#kafka_message_content deserialized"
                )
            end

            Procon.Helpers.inspect(
              Process.info(self()),
              "#{master_processing_id}_#{processing_id}@@PROCON EXCEPTION#Process.info in procon handle_message #{state.processor_consumer_config.name}/#{topic}/#{partition}/#{offset}"
            )

            Procon.Helpers.inspect(
              __STACKTRACE__,
              "#{master_processing_id}_#{processing_id}@@PROCON EXCEPTION#__STACKTRACE__ in procon handle_message #{state.processor_consumer_config.name}/#{topic}/#{partition}/#{offset}"
            )

            throw({:stop_procon_brod_client, processing_id, offset})
        end
      end

      {:ok, :commit, state}
    catch
      {:stop_procon_brod_client, processing_id, offset} ->
        case Map.get(state.processor_consumer_config, :bypass_exception, false) do
          true ->
            {:ok, :commit, state}

          false ->
            Procon.Helpers.inspect(
              "#{master_processing_id}_#{processing_id}@@PROCON STOPPED PROCESSOR #{state.processor_consumer_config.name}/#{topic}/#{partition}/#{offset}"
            )

            {:ok, state}
        end

      _ ->
        {:ok, state}
    end
  end

  def process_message(
        state,
        kafka_message_content,
        topic,
        partition,
        offset,
        processing_id
      ) do
    state.processor_consumer_config
    |> Procon.MessagesControllers.ProcessorConfig.find_entity_for_topic_pattern(topic)
    |> case do
      nil ->
        {:error, :process_message, :topic_not_listened}

      entity_config ->
        extract_procon_message_from_payload(
          kafka_message_content,
          Map.get(entity_config, :serialization, :json),
          entity_config,
          processing_id
        )
        |> case do
          {:error, :socket_closed_remotely} ->
            {:error, :process_message, :socket_closed_remotely}

          {:failed_connect, [{:to_address, {host, port}}, {:inet, [:inet], :econnreset}]} ->
            {:error, :process_message, {:failed_connect, host, port}}

          %Procon.Types.DebeziumMessage{} = procon_message ->
            route_message(
              procon_message,
              state.processor_consumer_config,
              topic,
              partition,
              offset,
              processing_id,
              entity_config,
              state
            )
            |> case do
              {:error, :unable_to_update_offset_in_ets} = e ->
                e

              {:error, _error} = e ->
                e

              :ok ->
                :ok
            end
        end
    end
  end

  def extract_procon_message_from_payload(
        kafka_message_content,
        :avro,
        entity_config,
        _processing_id
      ) do
    Avrora.decode(kafka_message_content)
    |> case do
      {:error, :socket_closed_remotely} ->
        {:error, :socket_closed_remotely}

      {:error, {:failed_connect, [{:to_address, _}]}} = e ->
        e

      decoded_payload ->
        decoded_payload
        |> elem(1)
        |> Procon.Helpers.map_keys_to_atom(
          %Procon.Types.DebeziumMessage{},
          Map.get(entity_config, :materialize_json_attributes, [])
        )
    end
  end

  def extract_procon_message_from_payload(
        kafka_message_content,
        :json,
        _entity_config,
        _processing_id
      ) do
    Jason.decode!(kafka_message_content, keys: :atoms)
  end

  def route_message(
        procon_message,
        processor_consumer_config,
        topic,
        partition,
        offset,
        processing_id,
        entity_config,
        state
      ) do
    apply(
      Map.get(entity_config, :messages_controller, Procon.MessagesControllers.Default),
      case Map.get(procon_message, :before, nil) do
        nil ->
          :create

        _ ->
          case Map.get(procon_message, :after, nil) do
            nil ->
              :delete

            _ ->
              :update
          end
      end,
      [
        procon_message,
        struct!(
          Procon.Types.BaseMethodOptions,
          entity_config
          |> Map.merge(%{
            datastore: processor_consumer_config.datastore,
            dynamic_topics_autostart_consumers:
              Map.get(
                processor_consumer_config,
                :dynamic_topics_autostart_consumers,
                false
              ),
            dynamic_topics_filters:
              Map.get(processor_consumer_config, :dynamic_topics_filters, []),
            ets_table_name: state.ets_table_name,
            offset: offset,
            partition: partition,
            processing_id: processing_id,
            processor_name: processor_consumer_config.name,
            processor_type:
              Map.get(
                processor_consumer_config,
                :type,
                processor_consumer_config.name
                |> Atom.to_string()
                |> String.split(".")
                |> Enum.at(3)
                |> case do
                  "Commands" ->
                    :command

                  "Operators" ->
                    :operator

                  "Queries" ->
                    :query
                end
              ),
            topic: topic
          })
        )
      ]
    )
  end
end
