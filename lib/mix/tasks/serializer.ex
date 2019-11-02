defmodule Mix.Tasks.Procon.Serializer do
  use Mix.Task
  import Mix.Generator

  @shortdoc "Generate an event serializer template"

  def run(args) do
    resource =
      OptionParser.parse(args, strict: [resource: :string]) |> elem(0) |> Keyword.get(:resource)

    service_name = Application.get_env(:procon, :service_name)
    app_module = Mix.Project.config()[:app] |> to_string() |> Macro.camelize()

    opts = [app_module: app_module, service_name: service_name, resource: resource]

    create_file(
      Path.join(["lib", "events_serializers", resource |> Macro.underscore()]) <> ".ex",
      serializer_template(opts)
    )
  end

  embed_template(:serializer, """
  defmodule <%= @app_module %>.EventsSerializers.<%= @resource %> do

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

    def build_message_event(event_status) do
      "<%= @service_name %>/<%= @resource |> Macro.underscore %>/\#{event_status}"
    end

    def topic do
      # add topic name where this resource will be produced
      ""
    end
  end
  """)
end
