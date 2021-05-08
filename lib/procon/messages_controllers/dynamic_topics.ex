defmodule Procon.MessagesController.DynamicTopics do
  alias Procon.MessagesControllers, as: PMC
  use PMC.Base

  def process_create(event, options) do
    event_data = get_in(event, ["body", Map.get(options, :event_version) |> to_string()])

    case options.dynamic_topics_filters do
      [_ | _] ->
        options.dynamic_topics_filters
        |> Enum.find(
          &(&1.processor === Map.get(event_data, "processor") &&
              &1.entity === Map.get(event_data, "entity"))
        )

      [] ->
        %{}
    end
    |> case do
      nil ->
        {:ok, Map.put(event, :record_from_db, false)}

      dynamic_topics_filter_config ->
        case super(event, options) do
          {:ok, updated_event} ->
            case (Map.get(options, :dynamic_topics_autostart_consumers, false) === false &&
                    Map.get(dynamic_topics_filter_config, :autostart) === true) ||
                   (Map.get(options, :dynamic_topics_autostart_consumers, false) === true &&
                      Map.get(dynamic_topics_filter_config, :autostart) !== false) do
              true ->
                PMC.ProcessorConfig.build_processor_config_for_topic_name(
                  Application.get_env(:procon, Processors)
                  |> Keyword.get(options.processor_name)
                  |> Keyword.get(:consumers)
                  |> Enum.find(&(&1.name == options.processor_name)),
                  Map.get(event_data, "topic_name")
                )
                |> PMC.ConsumersStarter.start_consumer_for_topic()

              _ ->
                nil
            end

            {:ok, updated_event}

          error ->
            error
        end
    end
  end
end
