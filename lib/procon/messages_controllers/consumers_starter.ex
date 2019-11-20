defmodule Procon.MessagesControllers.ConsumersStarter do
  def start() do
    Application.get_env(:procon, :consumers)
    |> Enum.each(&start_processor_consumers/1)
  end

  def start_processor_consumers(processor_config) do
    :ets.new(processor_config.name, [:set, :public, :named_table])

    Procon.MessagesControllers.Consumer.start(processor_config)
  end
end
