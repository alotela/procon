defmodule Procon.MessagesControllers.Consumer do
  require Record
  Record.defrecord :kafka_message, Record.extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  def init(_group_id, _args), do: {:ok, []}

  def handle_message(topic, partition, message, state) do
    IO.inspect(message, label: "message on #{topic}/#{partition}")
    IO.inspect(state)
    {:ok, :ack, State}
  end

  def start(topics) do
    :brod.start_link_group_subscriber(
      :brod_client_1,
      "group_id",
      topics,
      [offset_commit_policy: :commit_to_kafka_v2, offset_commit_interval_seconds: 5],
      [begin_offset: :earliest],
      __MODULE__,
     []
    )
    {:ok}
  end
end
