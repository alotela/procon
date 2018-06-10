defmodule Mix.Tasks.Procon.Controller do
  use Mix.Task
  import Mix.Generator

  @shortdoc "Create a consumer controller file"

  def run(args) do
    schema = OptionParser.parse(args) |> elem(0) |> Keyword.get(:schema)
    app_name = Mix.Project.config[:app] |> to_string
    app_module = app_name |> Macro.camelize
    host_app_main_repo = Mix.Ecto.parse_repo([]) |> List.first

    opts = [app_module: app_module, repo: host_app_main_repo, schema: schema]

    create_file Path.join(["lib", app_name, "message_controllers", schema |> Macro.underscore]) <> "_controller.ex", controller_template(opts)

    msg = "\ncontroller created. Open the file to check if everything is ok. Modify it to fit your needs.
\nDon't forget to create a migration and a schema for your resource with mix and ecto.\n\n"
    Mix.shell.info [msg]
  end

  embed_template :controller, """
  defmodule <%= @app_module %>.MessageControllers.<%= @schema %>Controller do
    @datastore <%= inspect @repo %> # repo for your resource
    @message_version 1 # handled message version
    @keys_mapping %{} # Map to remap fields name between messages and your db schema : %{"a" -> :b} will remap "a" found in your message to attribute :b. It's always "string" to "atom".
    @master_key nil # a tuple {:db_field, "message_key"}. if not nil, will be used by update and delete to find record 'where [db_field] = [message.message_key]'
    @model <%= @schema %> # Resource you will receive messages for
    @topic ""
    use Procon.MessageControllers.Base
  end
  """
end
