defmodule Procon.MessagesControllers.Consumer do
  require Record
  Record.defrecord :kafka_message, Record.extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  def init(_group_id, _args), do: {:ok, []}

  def handle_message(topic, partition, {_,_,_,_,_,message,_,_,_} = message_data, state) do
    case Poison.decode(message) do
      {:ok, decoded_message} -> route_message(decoded_message, partition)
      {:error, {:invalid, position, value}} ->
        IO.inspect(message_data, label: "invalid json at #{position}")
    end
    {:ok, :ack, state}
  end

  def route_message(decoded_message, partition) do
    case Application.get_env(:procon, :routes) |> Map.get(decoded_message |> Map.get("event")) do
      nil -> {:ok, :no_route_data}
      {topic, body_version, module, function} ->
        apply(module, function, [get_in(decoded_message, ["body", body_version]), {topic, partition, decoded_message}])
    end
  end

  def start(topics) do
    :brod.start_link_group_subscriber(
      Application.get_env(:procon, :broker_client_name),
      Application.get_env(:procon, :consumer_group_name),
      topics,
      [
        offset_commit_policy: :commit_to_kafka_v2,
        offset_commit_interval_seconds: Application.get_env(:procon, :offset_commit_interval_seconds)
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
