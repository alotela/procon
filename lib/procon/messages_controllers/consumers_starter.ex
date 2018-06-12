defmodule Procon.MessagesControllers.ConsumersStarter do
  def start() do
    Application.get_env(:procon, :routes)
    |> Map.values()
    |> Enum.map(&(elem(&1, 0)))
    |> Procon.MessagesControllers.Consumer.start()
  end
end
