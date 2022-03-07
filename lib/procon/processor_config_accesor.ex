defmodule Procon.ProcessorConfigAccessor do
  @spec activated_processors_configs :: [any]
  def activated_processors_configs(options \\ []) do
    Application.get_env(:procon, Processors)
    |> Enum.filter(
      &Enum.member?(
        Keyword.get(options, :processors_list, nil) ||
          Application.get_env(:procon, :activated_processors),
        elem(&1, 0)
      )
    )
    |> Enum.filter(fn {_processor_name, processor_config} ->
      case Keyword.get(options, :exclude_materialize_processors, false) do
        true ->
          case Keyword.get(processor_config, :is_materialize_operator, false) do
            true -> false
            false -> true
          end

        false ->
          true
      end
    end)
    |> Enum.filter(fn {_processor_name, processor_config} ->
      case {Keyword.get(options, :is_materialize_operator),
            Keyword.get(processor_config, :is_materialize_operator)} do
        {true, true} ->
          true

        {true, _} ->
          false

        {false, true} ->
          false

        _ ->
          true
      end
    end)
  end
end
