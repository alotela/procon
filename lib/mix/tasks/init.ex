defmodule Mix.Tasks.Procon.Init do
  use Mix.Task
  import Mix.Generator
  alias Mix.Tasks.Procon.Helpers

  @shortdoc "Initialize Procon in your project"
  @moduledoc ~S"""
  #Usage
  ```
     mix procon.init --processor MyApp.Processors.[Commands|Operators|Queries|PushViews].ProcessorName [--repo ProcessorPg] [--crud criud] [--html] [--public]
  ```
  """

  def run([]) do
    Helpers.info("You need to set --processor with maybe optional params :")

    Helpers.info(
      "mix procon.init --processor MyApp.Processors.[Commands|Operators|Queries|PushViews].ProcessorName [--repo ProcessorPg] [--crud criud] [--html] [--public]"
    )
  end

  def run(args) do
    app_name = Mix.Project.config()[:app] |> to_string

    app_web =
      Mix.Project.get!() |> Module.split() |> List.first() |> to_string() |> Kernel.<>("Web")

    processor_name =
      OptionParser.parse(args, strict: [processor: :string])
      |> elem(0)
      |> Keyword.get(:processor)

    crud =
      OptionParser.parse(args, strict: [crud: :string]) |> elem(0) |> Keyword.get(:crud, "criud")

    _jsonapi =
      OptionParser.parse(args, strict: [jsonapi: :boolean]) |> elem(0) |> Keyword.get(:jsonapi)

    generate_html =
      OptionParser.parse(args, strict: [html: :boolean])
      |> elem(0)
      |> Keyword.get(:html, false)

    public_controller =
      OptionParser.parse(args, strict: [public: :boolean])
      |> elem(0)
      |> Keyword.get(:public, false)

    processor_repo =
      OptionParser.parse(args, strict: [repo: :string])
      |> elem(0)
      |> Keyword.get(:repo, "#{Helpers.processor_to_controller(processor_name)}Pg")

    processor_default_topic =
      [
        app_name,
        "int",
        Helpers.processor_type(processor_name) |> Macro.underscore(),
        processor_name |> Helpers.processor_to_resource()
      ]
      |> Enum.join("-")

    migrations_path = Helpers.migrations_path(processor_name, processor_repo)

    Helpers.info("creating migrations directory #{migrations_path}")
    create_directory(migrations_path)

    migration_time = Helpers.Migrations.timestamp()

    procon_consumer_indexes_migration =
      Helpers.Migrations.generate_migration(
        migration_time + 1,
        "procon_consumer_indexes",
        migrations_path,
        processor_name,
        processor_repo,
        :procon_consumer_indexes
      )

    processor_entity_migration =
      Helpers.Migrations.generate_migration(
        migration_time + 4,
        "add_table_#{Helpers.processor_to_resource(processor_name)}",
        migrations_path,
        processor_name,
        processor_repo,
        :processor_entity
      )

    procon_dynamic_topics_migration =
      Helpers.Migrations.generate_migration(
        migration_time + 5,
        "procon_dynamic_topics",
        migrations_path,
        processor_name,
        processor_repo,
        :procon_dynamic_topics
      )

    procon_realtimes_migration =
      Helpers.Migrations.generate_migration(
        migration_time + 6,
        "procon_realtimes",
        migrations_path,
        processor_name,
        processor_repo,
        :procon_realtimes
      )

    processor_path = Helpers.processor_path(processor_name)

    Helpers.DefaultRepository.generate_repository(
      app_name,
      processor_name,
      processor_path,
      processor_repo
    )

    message_controllers_path = [processor_path, "message_controllers"] |> Path.join()

    Helpers.info("creating processor's message_controllers directory #{message_controllers_path}")
    message_controllers_path |> create_directory()

    schemas_path = [processor_path, "schemas"] |> Path.join()
    Helpers.info("creating schemas directory #{schemas_path}")
    schemas_path |> create_directory()
    Helpers.DefaultSchema.create_default_schema(processor_name, schemas_path)

    services_path = [processor_path, "services"] |> Path.join()
    Helpers.info("creating services directory #{services_path}")
    services_path |> create_directory()

    domain_service_path =
      create_default_services(services_path, processor_name, processor_repo, crud)

    services_infra_path = [services_path, "domain"] |> Path.join()
    Helpers.info("creating services infra directory #{services_infra_path}")
    services_infra_path |> create_directory()

    if Helpers.processor_type(processor_name) !== "Operators" do
      Helpers.WebDirectory.generate_web_directory(
        app_web,
        processor_name,
        processor_path,
        crud,
        generate_html,
        public_controller
      )

      Helpers.WebFile.generate_web_file(app_web, processor_name, processor_path)
      add_forward_to_router(app_name, processor_name)
    end

    generated_config_files =
      Helpers.ConfigFiles.generate_config_files(
        app_name,
        processor_name,
        processor_repo,
        processor_default_topic
      )

    controller_errors_path = Helpers.ControllersErrors.create_default_controllers_errors(app_web)
    controller_helpers_path = Helpers.ControllersErrors.create_controllers_helpers(app_web)
    [activated_path] = Helpers.Activated.generate_config_activated(processor_name)

    activate_processor(processor_name, activated_path)
    add_ecto_repos(processor_name, processor_repo)

    msg = """

    #{processor_name} procon processor added to your project.

    Generated files and directories :
        #{processor_path}: the new processor directory, where you put your code
        #{migrations_path}: where you find migration files for this processor
        #{generated_config_files |> hd()}: where you find config files for this processor
        #{procon_consumer_indexes_migration}: store topic/partition consumed indexes (for exactly once processing)
        #{procon_dynamic_topics_migration}: store dynamic topics from your system
        #{procon_realtimes_migration}: store realtimes messages you want to send
        #{processor_entity_migration}: default entity managed by this processor
        #{controller_errors_path}: list of errors for http reponse
        #{controller_helpers_path}: controllers helpers
        #{activated_path}: list of activated processors
        #{domain_service_path}: default services to manage service entity

      IMPORTANT!! To finish the setup:
      --------------------------------

      * add these lines in ./config/config.exs (if they are not already added):

        config :procon,
          brokers: [localhost: 9092],
          brod_client_config: [reconnect_cool_down_seconds: 10],
          nb_simultaneous_messages_to_send: 1000,
          offset_commit_interval_seconds: 5,
          consumers: []
        }

        for config <- "processors/**/*/config.exs" |> Path.expand(__DIR__) |> Path.wildcard() do
          import_config config
        end
        import_config "processors/activated.exs"

      * if it is not automatically added, add the processor repository #{
      processor_name |> Helpers.repo_name_to_module(processor_repo)
    } to "lib/#{app_name}/application.ex" in children array to start the repo when the application starts.

      * configure your processor in #{generated_config_files |> tl()}. This is where you add your kafka listeners.

    Now you can produce message :

      Procon.MessagesEnqueuers.Ecto.enqueue_event(event_data, EventSerializerModule, event_type)
      Procon.MessagesProducers.ProducersStarter.start_topic_production(nb_messages_to_batch, ProcessorRepository, topic)

    where :
      - event_data: a map/struct of your data for your message
      - event_type: a state (:created or :updated or :deleted)
      - nb_messages_to_batch: number of messages to send at the same time (use 1000 to start)
      - ProcessorRepository: the repository module where messages are stored
      - topic: the topic to start production for


    generate messages controller to consume events from kafka:
      mix procon.controller --schema SchemaName

    """

    Helpers.info(msg)
  end

  def create_default_services(services_path, processor_name, processor_repo, crud) do
    services_domain_path =
      [services_path, "domain", Helpers.processor_to_resource(processor_name)] |> Path.join()

    Helpers.info("creating services domain directory #{services_domain_path}")
    services_domain_path |> create_directory()

    if String.contains?(crud, "c") do
      services_domain_path
      |> Helpers.DefaultCreator.create_default_create_service(processor_name, processor_repo)
    end

    if String.contains?(crud, "r") || String.contains?(crud, "i") do
      services_domain_path
      |> Helpers.DefaultGetter.create_default_getter_service(processor_name, processor_repo, crud)
    end

    if String.contains?(crud, "u") do
      services_domain_path
      |> Helpers.DefaultUpdater.create_default_update_service(processor_name, processor_repo)
    end

    if String.contains?(crud, "d") do
      services_domain_path
      |> Helpers.DefaultDeleter.create_default_delete_service(processor_name, processor_repo)
    end

    services_domain_path
  end

  def activate_processor(processor_name, activated_file_path) do
    Helpers.info("adding processor to activated processors...")
    config_file_content = activated_file_path |> File.read!()

    new_content =
      String.replace(
        config_file_content,
        "activated_processors: [",
        "activated_processors: [\n    #{processor_name},"
      )

    :ok = File.write(activated_file_path, new_content)
    Helpers.info("Processor (1 line) added to #{config_file_content}")
  end

  @spec add_ecto_repos(atom | binary, atom | binary) :: any
  def add_ecto_repos(processor_name, processor_repo) do
    Helpers.info("adding processor's repo to :ecto_repos...")
    main_config_path = ["config", "config.exs"] |> Path.join()
    config_file_content = main_config_path |> File.read!()

    new_content =
      String.replace(
        config_file_content,
        "ecto_repos: [",
        "ecto_repos: [\n    #{processor_name |> Helpers.repo_name_to_module(processor_repo)},"
      )

    :ok = File.write(main_config_path, new_content)

    Helpers.info("Processor's repo (1 line) added to #{main_config_path}")
  end

  def add_forward_to_router(app_name, processor_name) do
    Helpers.info("adding forward route...")
    main_config_path = ["lib", "#{app_name}_web", "router.ex"] |> Path.join()
    config_file_content = main_config_path |> File.read!()

    new_content =
      String.replace(
        config_file_content,
        "use CalionsWeb, :router\n\n",
        "use CalionsWeb, :router\n\n  forward \"/#{
          Helpers.processor_type(processor_name) |> Macro.underscore()
        }/#{processor_name |> Helpers.short_processor_name()}\", #{processor_name}.Web.Router\n"
      )

    :ok = File.write(main_config_path, new_content)

    Helpers.info("forward route (1 line) added to #{main_config_path}")
  end
end
