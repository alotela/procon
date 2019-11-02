defmodule Procon.MessagesControllers.Consumer do
  require Record

  Record.defrecord(
    :kafka_message,
    Record.extract(:kafka_message, from_lib: "brod/include/brod.hrl")
  )

  def init(_group_id, _args), do: {:ok, []}

  def handle_message(
        _topic,
        partition,
        {:kafka_message, _offset, _key, message_content, _ts_type, _ts, _headers},
        state
      ) do
    IO.inspect(message_content, label: "message_content")

    case Jason.decode(message_content) |> IO.inspect() do
      {:ok, decoded_content} ->
        route_message(decoded_content, partition)

      {:error, %Jason.DecodeError{position: position, data: data}} ->
        IO.inspect(data, label: "invalid json at #{position}")
    end

    {:ok, :ack, state}
  end

  def route_message(decoded_content, partition) do
    case Application.get_env(:procon, :routes) |> Map.get(decoded_content |> Map.get("event")) do
      nil ->
        {:ok, :no_route_data}

      {topic, data_version, module, function} ->
        apply(module, function, [
          decoded_content
          |> Map.put("data_version", data_version)
          |> Map.put("partition", partition)
          |> Map.put("topic", topic)
        ])
    end
  end

  def start(topics) do
    :brod.start_link_group_subscriber(
      Application.get_env(:procon, :broker_client_name),
      Application.get_env(:procon, :consumer_group_name),
      topics,
      [
        offset_commit_policy: :commit_to_kafka_v2,
        offset_commit_interval_seconds:
          Application.get_env(:procon, :offset_commit_interval_seconds)
      ],
      [
        begin_offset: :earliest
      ],
      __MODULE__,
      []
    )

    {:ok}
  end
end
