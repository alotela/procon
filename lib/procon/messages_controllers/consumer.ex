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
    Enum.each(
      messages,
      fn {:kafka_message, _offset, _key, kafka_message_content, _ts_type, _ts, _headers} ->
        case Jason.decode(kafka_message_content) do
          {:ok, procon_message} ->
            route_message(procon_message, state.processor_config, topic, partition)

          {:error, %Jason.DecodeError{position: position, data: data}} ->
            IO.inspect(data, label: "invalid json at #{position}")
        end
      end
    )

    {:ok, :commit, state}
  end

  def route_message(procon_message, processor_config, topic, partition) do
    processor_config
    |> Procon.MessagesControllers.ProcessorConfig.find_entity_for_topic_pattern(topic)
    |> case do
      nil ->
        IO.inspect(processor_config, label: "topic not listened #{topic}")
        {:error, :topic_not_listened, processor_config}

      entity_config ->
        procon_message
        |> Map.get("event", :no_event_name)
        |> case do
          :no_event_name ->
            IO.inspect(procon_message, label: "no routage because no event name")

          event_name ->
            apply(
              Map.get(entity_config, :messages_controller, Procon.MessagesControllers.Default),
              case event_name |> String.to_atom() do
                :created -> :create
                :deleted -> :delete
                :updated -> :update
              end,
              [
                procon_message |> Map.put(:partition, partition),
                entity_config
                |> Map.merge(%{
                  dynamic_topics_autostart_consumers:
                    Map.get(processor_config, :dynamic_topics_autostart_consumers, false),
                  dynamic_topics_filters: Map.get(processor_config, :dynamic_topics_filters, []),
                  datastore: processor_config.datastore,
                  processor_config: processor_config,
                  processor_name: processor_config.name,
                  topic: topic
                })
              ]
            )
        end
    end
  end

  def stop(processor_name) do
    processor_name
    |> group_subscriber_name()
    |> Process.whereis()
    |> :brod_group_subscriber_v2.stop()

    processor_name
    |> client_name()
    |> :brod.stop_client()
  end

  def start(processor_config) do
    client_name = client_name(processor_config.name)

    :brod.start_client(
      Application.get_env(:procon, :brokers),
      client_name,
      Application.get_env(:procon, :brod_client_config)
    )

    start_consumer_for_topic(processor_config, nil, client_name)

    {:ok}
  end

  def start_consumer_for_topic(processor_config, group_id \\ nil, client_name \\ nil) do
    topics =
      Enum.reduce(processor_config.entities, [], fn entity_config, state ->
        case Map.get(entity_config, :dynamic_topic) do
          true ->
            Procon.MessagesController.Datastores.Ecto.find_dynamic_topics(
              processor_config.datastore,
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
          "PROCON NOTIFICATION : no topics consumer starter for processor #{processor_config.name}",
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
          client: client_name || client_name(processor_config.name),
          group_id:
            group_id || "#{processor_config.name}#{Map.get(processor_config, :group_id, "")}",
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
          init_data: %{processor_config: processor_config}
        })
        |> case do
          {:ok, pid} ->
            process_register_name = group_subscriber_name(processor_config.name)

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
