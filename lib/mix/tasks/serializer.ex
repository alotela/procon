defmodule Mix.Tasks.Procon.Serializer do
  use Mix.Task
  import Mix.Generator

  @shortdoc "Generate an event serializer template"
  @moduledoc ~S"""
  #Usage
  ```
     mix procon.serializer --processor MyDomain.Processors.ProcessorName --repo ProcessorPg --entity Entity --topic entity-topic
  ```
  """

  def info(msg) do
    msg = """
    #{msg}
    """

    Mix.shell().info([msg])
  end

  def run([]) do
    info("You need to set --processor, --entity and --repo params :")

    info(
      "mix procon.serializer --processor MyDomain.Processors.ProcessorName --repo ProcessorPg --entity Entity --topic entity-topic"
    )
  end

  def run(args) do
    processor_name =
      OptionParser.parse(args, strict: [processor: :string, repo: :string])
      |> elem(0)
      |> Keyword.get(:processor)

    processor_path = Mix.Tasks.Procon.Helpers.processor_path(processor_name)

    processor_repo =
      OptionParser.parse(args, strict: [repo: :string]) |> elem(0) |> Keyword.get(:repo)

    repo_module = Mix.Tasks.Procon.Helpers.repo_name_to_module(processor_name, processor_repo)

    entity =
      OptionParser.parse(args, strict: [entity: :string]) |> elem(0) |> Keyword.get(:entity)

    topic = OptionParser.parse(args, strict: [topic: :string]) |> elem(0) |> Keyword.get(:topic)

    opts = [
      entity: entity,
      processor_name: processor_name,
      repo_module: repo_module,
      topic: topic
    ]

    create_file(
      Path.join(
        (processor_path |> String.split("/")) ++
          ["events", "serializers", entity |> Macro.underscore()]
      ) <>
        ".ex",
      serializer_template(opts)
    )
  end

  embed_template(:serializer, """
  defmodule <%= @processor_name %>.Events.Serializers.<%= @entity %> do

    @doc \"\"\"
      Versions are used when we build the message map from event data
      The message will have as many versions in its payload attribute
    \"\"\"
    def message_versions, do: [1]

    def created(event_data, version) do
      case version do
        1 -> %{id: event_data.id}
        # 2 -> %{id: event_data.id2}
      end
    end

    def updated(event_data, version) do
      case version do
        1 -> %{id: event_data.id}
        # 2 -> %{id: event_data.id2}
      end
    end

    def destroyed(event_data, version) do
      case version do
        1 -> %{id: event_data.id}
        # 2 -> %{id: event_data.id2}
      end
    end

    def build_partition_key(event_data) do
      # use the best data for partitioning. IT MUST BE A STRING
      event_data.id
    end

    def topic do
      # add topic name where this entity will be produced
      "<%= @topic %>"
    end

    def repo, do: <%= @repo_module %>
  end
  """)
end
