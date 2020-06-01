defmodule Procon.MessagesControllers.ProcessorConfig do
  def find_entity_for_topic_pattern(processor_config, topic_pattern) do
    processor_config.entities
    |> Enum.find(
      &(
        Map.get(&1, :dynamic_topic) !== true && &1.topic == topic_pattern
        || (Map.get(&1, :dynamic_topic) === true && String.starts_with?(topic_pattern, &1.topic))
      )
    )
  end

  def find_entity_for_topic_name(processor_config, topic_name) do
    processor_config.entities
    |> Enum.find(
      &(
        Map.get(&1, :dynamic_topic) !== true && &1.topic == topic_name
        || (Map.get(&1, :dynamic_topic) === true && String.contains?(topic_name, &1.topic))
      )
    )
  end

  def build_entity_for_topic_name(processor_config, topic_name) do
    processor_config
    |> find_entity_for_topic_name(topic_name)
    |> Map.put(:topic, topic_name)
  end

  def build_processor_config_for_topic_name(processor_config, topic_name) do
    Map.put(
      processor_config,
      :entities,
      [build_entity_for_topic_name(processor_config, topic_name)]
    )
  end
end
