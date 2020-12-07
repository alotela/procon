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
    Process.sleep(5000)

    activated_consumers_configs()
    |> Enum.each(&start_processor_consumers/1)

    {:noreply, state}
  end

  defp start_processor_consumers(processor_consumer_config) do
    :ets.whereis(processor_consumer_config.name)
    |> case do
      :undefined ->
        :ets.new(processor_consumer_config.name, [:set, :public, :named_table])
        |> IO.inspect(
          label: "start_processor_consumers ets table for #{processor_consumer_config.name}"
        )

      _ ->
        IO.inspect(
          "ets table for #{processor_consumer_config.name} already started in procon::start_processor_consumers"
        )
    end

    Procon.MessagesControllers.Consumer.start(processor_consumer_config)
  end

  def activated_consumers_configs() do
    Application.get_env(:procon, Processors)
    |> Enum.filter(
      &Enum.member?(Application.get_env(:procon, :activated_processors), elem(&1, 0))
    )
    |> Enum.reduce([], &(Keyword.get(elem(&1, 1), :consumers, []) ++ &2))
  end

  def stop_processor(processor_name) do
    Procon.MessagesControllers.Consumer.stop(processor_name)

    :ets.whereis(processor_name)
    |> case do
      :undefined ->
        nil

      tab ->
        :ets.delete(tab)
    end
  end

  def start_processor_by_name(processor_name) do
    Application.get_env(:procon, Processors)
    |> Enum.find(&(elem(&1, 0) == processor_name))
    |> elem(1)
    |> Keyword.get(:consumers, [])
    |> List.first()
    |> start_processor_consumers()
  end
end
