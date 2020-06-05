defmodule Procon.MessagesControllers.ConsumersStarter do
  use GenServer

  def start_link(options) do
    GenServer.start_link(
      __MODULE__,
      Keyword.get(options, :initial_state, []),
      name: __MODULE__
    )
  end

  def start_consumer_for_topic(config, group_id \\ nil) do
    GenServer.cast(
      Procon.MessagesControllers.ConsumersStarter,
      {:start_consumer_for_topic, config, group_id}
    )
  end

  def start_activated_processors() do
    GenServer.cast(
      Procon.MessagesControllers.ConsumersStarter,
      {:start_activated_processors}
    )
  end

  ## GenServer callbacks
  def init(initial_state) do
    {:ok, initial_state}
  end

  def handle_cast({:start_consumer_for_topic, config, group_id}, state) do
    Procon.MessagesControllers.Consumer.start_consumer_for_topic(config, group_id)
    {:noreply, state}
  end

  def handle_cast({:start_activated_processors}, state) do
    activated_consumers()
    |> Enum.each(&start_processor_consumers/1)

    {:noreply, state}
  end

  defp start_processor_consumers(processor_config) do
    :ets.new(processor_config.name, [:set, :public, :named_table])

    Procon.MessagesControllers.Consumer.start(processor_config)
  end

  def activated_consumers() do
    Application.get_env(:procon, Processors)
    |> Enum.filter(
      &Enum.member?(Application.get_env(:procon, :activated_processors), elem(&1, 0))
    )
    |> Enum.reduce([], &(Keyword.get(elem(&1, 1), :consumers, []) ++ &2))
  end
end
