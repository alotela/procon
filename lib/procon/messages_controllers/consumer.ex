defmodule Procon.MessagesControllers.Consumer do
  require Record

  Record.defrecord(
    :kafka_message,
    Record.extract(:kafka_message, from_lib: "brod/include/brod.hrl")
  )

  def init(consumer_config, init_data), do: {:ok, Map.merge(consumer_config, init_data)}

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
    processor_config.entities
    |> Enum.find(nil, &(&1.topic == topic))
    |> case do
      nil ->
        IO.inspect(processor_config, label: "topic not listened #{topic}")
        {:error, :topic_not_listened, processor_config}

      entity_config ->
        apply(
          Map.get(entity_config, :messages_controller, Procon.MessagesControllers.Default),
          case procon_message |> Map.get("event") |> String.to_atom() do
            :created -> :create
            :deleted -> :delete
            :updated -> :update
          end,
          [
            procon_message
            |> Map.put(:partition, partition),
            entity_config
            |> Map.put(:datastore, processor_config.datastore)
            |> Map.put(:processor_name, processor_config.name)
          ]
        )
    end
  end

  def start(processor_config) do
    IO.inspect(processor_config, label: "starting processor with config")

    client_name =
      processor_config.name
      |> to_string()
      |> String.downcase()
      |> String.replace(".", "_")
      |> String.to_atom()

    :brod.start_client(
      Application.get_env(:procon, :brokers),
      client_name,
      Application.get_env(:procon, :brod_client_config)
    )
    |> IO.inspect(label: "starting client for #{client_name}")

    :brod.start_link_group_subscriber_v2(%{
      client: client_name,
      group_id: processor_config.name |> to_string(),
      topics:
        Enum.reduce(processor_config.entities, [], &[&1.topic | &2])
        |> IO.inspect(label: "topics for #{processor_config.name}"),
      group_config: [
        offset_commit_policy: :commit_to_kafka_v2,
        offset_commit_interval_seconds:
          Application.get_env(:procon, :offset_commit_interval_seconds) |> IO.inspect()
      ],
      consumer_config: [
        begin_offset: :earliest
      ],
      cb_module: __MODULE__,
      init_data: %{processor_config: processor_config}
    })
    |> IO.inspect(label: "brod.start_link_group_subscriber_v2")

    {:ok}
  end
end
