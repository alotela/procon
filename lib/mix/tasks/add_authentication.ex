defmodule Mix.Tasks.Procon.AddAuthentication do
  use Mix.Task
  import Mix.Generator
  alias Mix.Tasks.Procon.Helpers

  @shortdoc "Add Calions authentication to a command or view processor"
  @moduledoc ~S"""
  #Usage
  ```
     mix procon.add_authentication --processor MyApp.Processors.ProcessorType.ProcessorName [--repo ProcessorNamePg]
  ```
  """

  def info(msg) do
    msg = """
    #{msg}
    """

    Mix.shell().info([msg])
  end

  def run([]) do
    info("You need to set --processor and optional --repo param :")

    info(
      "mix procon.add_authentication --processor MyApp.Processors.ProcessorType.ProcessorName [--repo ProcessorNamePg]"
    )
  end

  def run(args) do
    processor_name =
      OptionParser.parse(args, strict: [processor: :string])
      |> elem(0)
      |> Keyword.get(:processor)

    processor_repo =
      OptionParser.parse(args, strict: [repo: :string])
      |> elem(0)
      |> Keyword.get(:repo, "#{Helpers.processor_to_controller(processor_name)}Pg")

    config_path = add_topic_config(processor_name)
    Helpers.info("added topics to listen in processor's config file #{config_path}")
    migration_file_path = add_migrations(processor_name, processor_repo)
    Helpers.info("creating migrations file #{migration_file_path}")
    router_file_path = add_api_routes(processor_name, processor_repo)
    Helpers.info("added plugs and routes to router's file #{router_file_path}")
  end

  def add_topic_config(processor_name) do
    config_file_path = [Helpers.config_directory(processor_name), "config.exs"] |> Path.join()
    config_file_content = config_file_path |> File.read!()

    new_content =
      String.replace(
        config_file_content,
        "entities: [\n          ",
        "entities: [\n          %{\n            event_version: 1,\n            keys_mapping: %{},\n            master_key: nil,\n            model: Calions.AuthenticatedClients.Schemas.AuthenticatedClient,\n            topic: \"calions-int-operators-authenticated_clients\"\n          },\n          "
      )

    :ok = File.write(config_file_path, new_content)

    config_file_path
  end

  def add_migrations(processor_name, processor_repo) do
    migration_time = Helpers.Migrations.timestamp()
    migrations_path = Helpers.migrations_path(processor_name, processor_repo)

    create_file(
      Path.join(migrations_path, "#{migration_time}_add_authenticated_clients_table.exs"),
      calions_authentications_migrations_template(
        processor_name: processor_name,
        table: Helpers.processor_to_resource(processor_name)
      )
    )

    migrations_path
  end

  embed_template(
    :calions_authentications_migrations,
    from_file:
      Path.join([
        __ENV__.file |> Path.dirname(),
        "helpers",
        "templates",
        "calions_authentications_migrations.eex"
      ])
  )

  def add_api_routes(processor_name, processor_repo) do
    processor_atom = Helpers.short_processor_name(processor_name)

    pipelines = """
    pipeline :#{processor_name |> Helpers.processor_type() |> Macro.underscore()}_#{
      processor_atom
    }_auth do
        plug(Calions.Plugs.AuthenticatedAccountPlug,
          repo: #{processor_name}.Repositories.#{processor_repo},
          schema: Calions.AuthenticatedClients.Schemas.AuthenticatedClient
        )
      end

      pipeline :#{processor_name |> Helpers.processor_type() |> Macro.underscore()}_#{
      processor_atom
    }_auth_private do
        plug(Calions.Plugs.EnsureAuthenticatedPlug)
        plug(Calions.Plugs.EnsureAuthenticatedUserPlug)
      end
    """

    router_file_path = Helpers.router_file_path(processor_name)
    router_file_content = router_file_path |> File.read!()

    new_content =
      String.replace(
        router_file_content,
        "scope \"/api\", Calions.Processors",
        "#{pipelines}\n  scope \"/api\", Calions.Processors",
        global: false
      )
      |> String.replace(
        "pipe_through([:api, :jsonapi",
        "pipe_through([:api, :jsonapi, :#{
          processor_name |> Helpers.processor_type() |> Macro.underscore()
        }_#{processor_atom}_auth",
        global: false
      )
      |> String.replace(
        "scope \"/private\", Private, as: :private do\n",
        "scope \"/private\", Private, as: :private do\n      pipe_through([:#{
          processor_name |> Helpers.processor_type() |> Macro.underscore()
        }_#{processor_atom}_auth_private])\n",
        global: false
      )

    :ok = File.write(router_file_path, new_content)

    router_file_path
  end
end
