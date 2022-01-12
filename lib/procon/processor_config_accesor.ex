defmodule Procon.ProcessorConfigAccessor do
  @spec activated_processors_config :: [any]
  def activated_processors_config() do
    Application.get_env(:procon, Processors)
    |> Enum.filter(
      &Enum.member?(Application.get_env(:procon, :activated_processors), elem(&1, 0))
    )
  end

  @spec activated_processors_producers_config :: %{optional(atom()) => map()}
  def activated_processors_producers_config() do
    Application.get_env(:procon, Processors)
    |> Enum.reduce(%{}, fn {processor_name, processor_config}, producers_config ->
      case Enum.member?(
             Application.get_env(:procon, :activated_processors),
             processor_name
           ) do
        true ->
          Keyword.get(processor_config, :producers)
          |> case do
            nil ->
              producers_config

            producers_config ->
              Map.put(producers_config, processor_name, producers_config)
          end

        false ->
          producers_config
      end
    end)
  end

  def activated_consumers_configs() do
    activated_processors_config()
    |> Enum.reduce([], &(Keyword.get(elem(&1, 1), :consumers, []) ++ &2))
  end
end
