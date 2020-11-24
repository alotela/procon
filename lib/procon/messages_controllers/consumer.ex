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
      for {:kafka_message, offset, _key, kafka_message_content, _ts_type, _ts, _headers} <- messages do
        try do
          {:ok, procon_message} = Jason.decode(kafka_message_content)
          route_message(procon_message, state.processor_config, topic, partition)
        rescue
          e ->
            Procon.Helpers.inspect(e, "@@exception in procon handle_message @#{topic}/#{partition}/#{offset}@@")
            Procon.Helpers.inspect(kafka_message_content, "@@kafka_message_content@#{topic}/#{partition}/#{offset}@@")
            Procon.Helpers.inspect(Process.info(self()), :process_info)
            Procon.Helpers.inspect(__STACKTRACE__, "__STACKTRACE__")
            throw(:stop_procon)
        end
      end
      {:ok, :commit, state}
    catch
      :stop_procon ->
        case Map.get(state.processor_config, :bypass_exception, false) do
          true ->
            {:ok, :commit, state}
          false ->
            Procon.Helpers.inspect("procon stopped processor #{state.processor_config.name}")
            Procon.MessagesControllers.ConsumersStarter.stop_processor(state.processor_config.name)
            {:ok, state}
        end
      _ ->
        {:ok, state}
    end
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

    :ets.lookup(:procon_consumer_group_subscribers, group_subscriber_name(processor_name))
    |> Procon.Parallel.pmap(fn {_proc, pid} -> :brod_group_subscriber_v2.stop(pid) end)

    :ets.delete(:procon_consumer_group_subscribers, group_subscriber_name(processor_name))

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
            :ets.insert(
              :procon_consumer_group_subscribers,
              {group_subscriber_name(processor_config.name), pid}
            )

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
