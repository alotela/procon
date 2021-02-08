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
               state.processor_consumer_config.name,
               processing_id
             ))
          |> case do
            true ->
              state.processor_consumer_config
              |> Procon.MessagesControllers.ProcessorConfig.find_entity_for_topic_pattern(topic)
              |> case do
                nil ->
                  Procon.Helpers.inspect(
                    kafka_message_content,
                    "#{processing_id}@@PROCON FLOW : topic not listened : #{
                      state.processor_consumer_config.name
                    }/#{topic}/#{partition}/#{offset}."
                  )

                entity_config ->
                  procon_message = case Map.get(entity_config, :serialization, :json) do
                    :avro ->
                      Avrora.decode(kafka_message_content)
                      |> elem(1)
                      |> Procon.Helpers.map_keys_to_atom()
                    :json ->
                      Jason.decode!(kafka_message_content, keys: :atoms)
                  end

                  route_message(
                    procon_message,
                    state.processor_consumer_config,
                    topic,
                    partition,
                    offset,
                    processing_id,
                    entity_config
                  )
                  |> case do
                    {:error, :unable_to_update_offset_in_ets} ->
                      Procon.Helpers.inspect(
                        kafka_message_content,
                        "#{processing_id}@@PROCON FLOW : unable to update offset in ets : #{
                          state.processor_consumer_config.name
                        }/#{topic}/#{partition}/#{offset}."
                      )

                      throw({:stop_procon, processing_id, offset})

                    {:error, error} ->
                      Procon.Helpers.inspect(
                        error,
                        "#{processing_id}@@PROCON FLOW : error from processing : #{
                          state.processor_consumer_config.name
                        }/#{topic}/#{partition}/#{offset}."
                      )

                      throw({:stop_procon, processing_id, offset})

                    :ok ->
                      nil
                  end
              end

            false ->
              Procon.Helpers.inspect(
                kafka_message_content,
                "#{processing_id}@@PROCON FLOW : message already processed : #{
                  state.processor_consumer_config.name
                }/#{topic}/#{partition}/#{offset}."
              )
          end
        rescue
          e ->
            Procon.Helpers.inspect(
              e,
              "#{processing_id}@@PROCON EXCEPTION#exception in procon handle_message #{
                state.processor_consumer_config.name
              }/#{topic}/#{partition}/#{offset}"
            )

            Procon.Helpers.inspect(
              kafka_message_content,
              "#{processing_id}@@PROCON EXCEPTION#kafka_message_content in procon handle_message #{
                state.processor_consumer_config.name
              }/#{topic}/#{partition}/#{offset}"
            )

            Procon.Helpers.inspect(
              Process.info(self()),
              "#{processing_id}@@PROCON EXCEPTION#Process.info in procon handle_message #{
                state.processor_consumer_config.name
              }/#{topic}/#{partition}/#{offset}"
            )

            Procon.Helpers.inspect(
              __STACKTRACE__,
              "#{processing_id}@@PROCON EXCEPTION#__STACKTRACE__ in procon handle_message #{
                state.processor_consumer_config.name
              }/#{topic}/#{partition}/#{offset}"
            )

            throw({:stop_procon, processing_id, offset})
        end
      end

      {:ok, :commit, state}
    catch
      {:stop_procon, processing_id, offset} ->
        case Map.get(state.processor_consumer_config, :bypass_exception, false) do
          true ->
            {:ok, :commit, state}

          false ->
            Procon.Helpers.inspect(
              "#{processing_id}@@PROCON STOPPED PROCESSOR #{state.processor_consumer_config.name}/#{
                topic
              }/#{partition}/#{offset}"
            )

            Procon.MessagesControllers.ConsumersStarter.stop_processor(
              state.processor_consumer_config.name
            )

            {:ok, state}
        end

      _ ->
        {:ok, state}
    end
  end

  def route_message(
        procon_message,
        processor_consumer_config,
        topic,
        partition,
        offset,
        processing_id,
        entity_config
      ) do
    apply(
      Map.get(entity_config, :messages_controller, Procon.MessagesControllers.Default),
      case procon_message.op do
        "c" -> :create
        "d" -> :delete
        "u" -> :update
      end,
      [
        procon_message,
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
          offset: offset,
          partition: partition,
          processing_id: processing_id,
          processor_name: processor_consumer_config.name,
          topic: topic
        })
      ]
    )
  end

  def stop(processor_name) do
    processor_name
    |> group_subscriber_name()
    |> Process.whereis()

    :ets.lookup(:procon_consumer_group_subscribers, group_subscriber_name(processor_name))
    |> Procon.Parallel.pmap(fn {_proc, pid} -> :brod_group_subscriber_v2.stop(pid) end)

    :ets.delete(:procon_consumer_group_subscribers, group_subscriber_name(processor_name))

    processor_name
    |> client_name()
    |> :brod.stop_client()
  end

  def start(processor_consumer_config) do
    client_name = client_name(processor_consumer_config.name)

    :brod.start_client(
      Application.get_env(:procon, :brokers),
      client_name,
      Application.get_env(:procon, :brod_client_config)
    )

    start_consumer_for_topic(processor_consumer_config, nil, client_name)

    {:ok}
  end

  def start_consumer_for_topic(processor_consumer_config, group_id \\ nil, client_name \\ nil) do
    topics =
      Enum.reduce(processor_consumer_config.entities, [], fn entity_config, state ->
        case Map.get(entity_config, :dynamic_topic) do
          true ->
            Procon.MessagesController.Datastores.Ecto.find_dynamic_topics(
              processor_consumer_config.datastore,
              entity_config.topic
            )

          _ ->
            [entity_config.topic]
        end
        |> Kernel.++(state)
      end)

    case topics do
      [] ->
        IO.inspect(
          "PROCON NOTIFICATION : no topics consumer starter for processor #{
            processor_consumer_config.name
          }",
          syntax_colors: [
            atom: :red,
            binary: :red,
            boolean: :red,
            list: :red,
            map: :red,
            number: :red,
            regex: :red,
            string: :red,
            tuple: :red
          ]
        )

        nil

      _ ->
        :brod.start_link_group_subscriber_v2(%{
          client: client_name || client_name(processor_consumer_config.name),
          group_id:
            group_id ||
              "#{processor_consumer_config.name}#{
                Map.get(processor_consumer_config, :group_id, "")
              }",
          topics: topics,
          group_config: [
            offset_commit_policy: :commit_to_kafka_v2,
            offset_commit_interval_seconds:
              Application.get_env(:procon, :offset_commit_interval_seconds)
          ],
          consumer_config: [
            begin_offset: :earliest
          ],
          cb_module: __MODULE__,
          init_data: %{processor_consumer_config: processor_consumer_config}
        })
        |> case do
          {:ok, pid} ->
            process_register_name = group_subscriber_name(processor_consumer_config.name)

            case Process.alive?(pid) do
              false ->
                IO.inspect(
                  pid,
                  label:
                    "trying to register group_subscriber #{process_register_name} but it is not alive.",
                  syntax_colors: [string: :magenta]
                )

              true ->
                case Process.whereis(process_register_name) do
                  xpid when is_pid(xpid) ->
                    IO.inspect(
                      xpid,
                      label:
                        "trying to register group_subscriber #{process_register_name} but another process is already registered for this name",
                      syntax_colors: [string: :magenta]
                    )

                  nil ->
                    IO.inspect(
                      pid,
                      label:
                        "registering group_subscriber #{process_register_name}.....................................",
                      syntax_colors: [string: :blue]
                    )

                    Process.register(pid, process_register_name)
                end
            end

          {:error, error} ->
            IO.inspect(error,
              binaries: :as_strings,
              label: "PROCON ALERT : unable to start a consumer",
              limit: :infinity,
              pretty: true,
              syntax_colors: [
                atom: :magenta,
                binary: :magenta,
                boolean: :magenta,
                list: :magenta,
                map: :magenta,
                null: :magenta,
                number: :magenta,
                regex: :magenta,
                string: :magenta,
                tuple: :magenta
              ]
            )
        end
    end

    {:ok}
  end

  def client_name(processor_name) do
    processor_name
    |> to_string()
    |> String.downcase()
    |> String.replace(".", "_")
    |> String.to_atom()
  end

  def group_subscriber_name(processor_name) do
    processor_name
    |> to_string()
    |> String.downcase()
    |> String.replace(".", "_")
    |> Kernel.<>("_group_subscriber")
    |> String.to_atom()
  end
end
