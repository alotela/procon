defmodule Procon.MessagesControllers.ConsumersStarter do
  def start() do
    activated_consumers()
    |> Enum.each(&start_processor_consumers/1)
  end

  def start_processor_consumers(processor_config) do
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
