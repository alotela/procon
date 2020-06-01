defmodule Mix.Tasks.Procon.Helpers.Serializers do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def create_dynamic_topic(processor_name, processor_repo, schema_directory_path) do
    file_path = Path.join([schema_directory_path, "dynamic_topic.ex"])

    unless File.exists?(file_path) do
      Helpers.info("creating dynamic topic serializer file #{file_path}")

      create_file(
        file_path,
        dynamic_topic_template(
          processor_module: processor_name,
          processor_repo: Helpers.repo_name_to_module(processor_name, processor_repo)
        )
      )
    end
  end

  embed_template(
    :dynamic_topic,
    """
    defmodule <%= @processor_module %>.Events.Serializers.DynamicTopic do
      use Procon.Serializers.DynamicTopicBase,
        repo: <%= @processor_repo %>
    end
    """
  )
end
