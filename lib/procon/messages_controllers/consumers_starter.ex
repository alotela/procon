defmodule Procon.MessagesControllers.ConsumersStarter do
  def start() do
    :procon_consumer_indexes = :ets.new(:procon_consumer_indexes, [:set, :public, :named_table])

    Application.get_env(:procon, :routes)
    |> Map.values()
    |> Enum.map(&elem(&1, 0))
    |> Procon.MessagesControllers.Consumer.start()
  end
end
