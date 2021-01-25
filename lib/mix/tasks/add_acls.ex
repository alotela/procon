defmodule Mix.Tasks.Procon.AddAcls do
  use Mix.Task
  import Mix.Generator
  alias Mix.Tasks.Procon.Helpers

  @shortdoc "Add Calions acls to a command or view processor"
  @moduledoc ~S"""
  #Usage
  ```
     mix procon.add_acls --processor MyApp.Processors.ProcessorType.ProcessorName [--repo ProcessorNamePg]
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
      "mix procon.add_acls --processor MyApp.Processors.ProcessorType.ProcessorName [--repo ProcessorNamePg]"
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

    topic =
      "calions-int-#{processor_name |> Helpers.processor_type() |> Macro.underscore()}-#{
        processor_name |> Helpers.short_processor_name()
      }-group_acls"

    config_path = add_topic_config(processor_name, topic)
    Helpers.info("added topics to listen in processor's config file #{config_path}")
    migration_file_path = add_migrations(processor_name, processor_repo)
    Helpers.info("created migrations file #{migration_file_path}")
    message_controllers_path = add_group_acls_message_controller(processor_name)
    Helpers.info("created group_acls message controller file #{message_controllers_path}")
    repository_file_path = add_group_acls_repository(processor_name, processor_repo)
    Helpers.info("created group_acls message repository file #{repository_file_path}")
    group_acls_value_object_path = add_value_object_group_acls(processor_name)
    Helpers.info("created group_acls value objbect file #{group_acls_value_object_path}")
    router_file_path = add_api_routes(processor_name, processor_repo)
    Helpers.info("added plugs and routes to router's file #{router_file_path}")

    Helpers.info("You must add the topic #{topic} to kafka")
  end

  def add_value_object_group_acls(processor_name) do
    file_path =
      [Helpers.processor_path(processor_name), "value_objects", "group_acls.ex"]
      |> Path.join()

    create_directory(file_path |> Path.dirname())

    create_file(
      file_path,
      group_acls_value_object_template(processor_name: processor_name)
    )

    file_path
  end

  embed_template(
    :group_acls_value_object,
    from_file:
      Path.join([
        __ENV__.file |> Path.dirname(),
        "helpers",
        "templates",
        "calions_group_acls_value_object.eex"
      ])
  )

  def add_group_acls_repository(processor_name, processor_repo) do
    file_path =
      [Helpers.processor_path(processor_name), "repositories", "group_acls.ex"]
      |> Path.join()

    create_file(
      file_path,
      group_acls_repository_template(
        processor_name: processor_name,
        processor_repo: processor_repo
      )
    )

    file_path
  end

  embed_template(
    :group_acls_repository,
    from_file:
      Path.join([
        __ENV__.file |> Path.dirname(),
        "helpers",
        "templates",
        "group_acls_repository.eex"
      ])
  )

  def add_group_acls_message_controller(processor_name) do
    message_controllers_path =
      [Helpers.processor_path(processor_name), "message_controllers", "group_acls.ex"]
      |> Path.join()

    create_file(
      message_controllers_path,
      group_acls_template(
        processor_name: processor_name,
        group_acl_type:
          "#{
            processor_name
            |> Helpers.processor_type()
            |> Macro.underscore()
          }_#{
            processor_name
            |> Helpers.short_processor_name()
          }"
      )
    )

    message_controllers_path
  end

  embed_template(
    :group_acls,
    from_file:
      Path.join([
        __ENV__.file |> Path.dirname(),
        "helpers",
        "templates",
        "group_acls.eex"
      ])
  )

  def add_topic_config(processor_name, topic) do
    config_file_path = [Helpers.config_directory(processor_name), "config.exs"] |> Path.join()
    config_file_content = config_file_path |> File.read!()

    new_content =
      String.replace(
        config_file_content,
        "entities: [",
        """
        entities: [
                  %{
                    event_version: 1,
                    keys_mapping: %{id: :app_group_id, name: :group_name},
                    master_key: {:app_group_id, :id},
                    messages_controller: #{processor_name}.MessageControllers.GroupAcls,
                    model: Calions.GroupAcls.Schemas.GroupAcl,
                    topic: "calions-int-operators-app_groups"
                  },
                  %{
                    event_version: 1,
                    keys_mapping: %{},
                    master_key: nil,
                    model: Calions.GroupAcls.Schemas.SelectedUserAppGroup,
                    topic: "calions-int-operators-selected_user_app_groups"
                  },
        """
      )
      |> String.replace(
        "relation_topics: %{",
        "relation_topics: %{:group_acls => {:id, :\"#{topic}\"}, "
      )

    :ok = File.write(config_file_path, new_content)

    config_file_path
  end

  def add_migrations(processor_name, processor_repo) do
    migration_time = Helpers.Migrations.timestamp()
    migrations_path = Helpers.migrations_path(processor_name, processor_repo)

    create_file(
      Path.join(migrations_path, "#{migration_time}_add_acls_tables.exs"),
      calions_authentications_migrations_template(processor_name: processor_name)
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
        "calions_acls_migrations.eex"
      ])
  )

  def add_api_routes(processor_name, processor_repo) do
    processor_atom = Helpers.short_processor_name(processor_name)

    forward = """
          Calions.GroupAcls.Web.Router.forward_acls(
            "/group_acls",
            #{processor_name}.Repositories.#{processor_repo},
            Calions.GroupAcls.Schemas.GroupAcl,
            #{processor_name}.Repositories.GroupAcls,
            #{processor_name}.ValueObjects.GroupAcls
          )
    """

    router_file_path = Helpers.router_file_path(processor_name)
    router_file_content = router_file_path |> File.read!()

    new_content =
      String.replace(
        router_file_content,
        ", :router",
        ", :router\n  require Calions.GroupAcls.Web.Router",
        global: false
      )
      |> String.replace(
        "pipe_through([:#{processor_name |> Helpers.processor_type() |> Macro.underscore()}_#{
          processor_atom
        }_auth_private])",
        "pipe_through([:#{processor_name |> Helpers.processor_type() |> Macro.underscore()}_#{
          processor_atom
        }_auth_private])\n\n#{forward}",
        global: false
      )

    :ok = File.write(router_file_path, new_content)

    router_file_path
  end
end
