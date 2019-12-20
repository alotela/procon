defmodule Mix.Tasks.Procon.Init do
  use Mix.Task
  import Mix.Generator
  alias Mix.Tasks.Procon.Helpers

  @shortdoc "Initialize Procon in your project"
  @moduledoc ~S"""
  #Usage
  ```
     mix procon.init --processor MyDomain.Processors.ProcessorName --repo ProcessorPg --crud criud
  ```
  """

  def run([]) do
    Helpers.info("You need to set --processor and --repo params :")

    Helpers.info(
      "mix procon.init --processor MyDomain.Processors.ProcessorName --repo ProcessorPg --crud criud"
    )
  end

  def run(args) do
    app_name = Mix.Project.config()[:app] |> to_string

    app_web =
      Mix.Project.get!() |> Module.split() |> List.first() |> to_string() |> Kernel.<>("Web")

    processor_name =
      OptionParser.parse(args, strict: [processor: :string, repo: :string])
      |> elem(0)
      |> Keyword.get(:processor)

    crud = OptionParser.parse(args, strict: [crud: :string]) |> elem(0) |> Keyword.get(:crud)

    _jsonapi =
      OptionParser.parse(args, strict: [jsonapi: :boolean]) |> elem(0) |> Keyword.get(:jsonapi)

    processor_repo =
      OptionParser.parse(args, strict: [repo: :string]) |> elem(0) |> Keyword.get(:repo)

    processor_default_entity =
      processor_name
      |> Helpers.processor_to_controller()
      |> Inflex.singularize()

    processor_default_topic =
      [
        app_name,
        "int-evt",
        processor_name |> Helpers.processor_to_resource()
      ]
      |> Enum.join("-")

    migrations_path = Path.join(["priv", processor_repo |> Macro.underscore(), "migrations"])

    Helpers.info("creating migrations directory #{migrations_path}")
    create_directory(migrations_path)

    migration_time = Helpers.Migrations.timestamp()

    procon_producer_messages_migration =
      Helpers.Migrations.generate_migration(
        migration_time,
        "procon_producer_messages",
        migrations_path,
        processor_name,
        processor_repo,
        :procon_producer_messages
      )

    procon_consumer_indexes_migration =
      Helpers.Migrations.generate_migration(
        migration_time + 1,
        "procon_consumer_indexes",
        migrations_path,
        processor_name,
        processor_repo,
        :procon_consumer_indexes
      )

    procon_producer_balancings_migration =
      Helpers.Migrations.generate_migration(
        migration_time + 2,
        "procon_producer_balancings",
        migrations_path,
        processor_name,
        processor_repo,
        :procon_producer_balancings
      )

    procon_producer_indexes_migration =
      Helpers.Migrations.generate_migration(
        migration_time + 3,
        "procon_producer_indexes",
        migrations_path,
        processor_name,
        processor_repo,
        :procon_producer_indexes
      )

    processor_entity_migration =
      Helpers.Migrations.generate_migration(
        migration_time + 4,
        "processor_entity",
        migrations_path,
        processor_name,
        processor_repo,
        :processor_entity
      )

    processor_path =
      Path.join([
        "lib",
        "processors",
        processor_name |> String.split(".") |> List.last() |> Macro.underscore()
      ])

    Helpers.DefaultRepository.generate_repository(app_name, processor_name, processor_path, processor_repo)

    events_path = [processor_path, "events"] |> Path.join()

    Helpers.info("creating processor's events directory #{events_path}")
    events_path |> create_directory()

    serializers_path = [events_path, "serializers"] |> Path.join()

    Helpers.info(
      "creating processor's events serializers directory #{serializers_path}"
    )

    serializers_path |> create_directory()

    schemas_path = [processor_path, "schemas"] |> Path.join()
    Helpers.info("creating schemas directory #{schemas_path}")
    schemas_path |> create_directory()
    Helpers.DefaultSchema.create_default_schema(processor_name, schemas_path)

    services_path = [processor_path, "services"] |> Path.join()
    Helpers.info("creating services directory #{services_path}")
    services_path |> create_directory()

    create_default_services(services_path, processor_name, processor_repo, crud)

    services_infra_path = [services_path, "domain"] |> Path.join()
    Helpers.info("creating services infra directory #{services_infra_path}")
    services_infra_path |> create_directory()

    Helpers.WebDirectory.generate_web_directory(app_web, processor_name, processor_path, crud)
    Helpers.WebFile.generate_web_file(app_web, processor_name, processor_path)

    generated_config_files = Helpers.ConfigFiles.generate_config_files(app_name, processor_name, processor_repo)

    Mix.Tasks.Procon.Serializer.run(
      ["--processor",
       processor_name,
        "--repo",
        processor_repo,
        "--entity",
        processor_default_entity,
        "--topic",
        processor_default_topic
      ]
    )

    controller_errors_path = Helpers.ControllersErrors.create_default_controllers_errors(app_web)
    controller_helpers_path = Helpers.ControllersErrors.create_controllers_helpers(app_web)

    msg = """

    #{processor_name} procon processor added to your project.

    Generated files and directories :
        #{processor_path}: the new processor directory, where you put your code
        #{migrations_path}: where you find migration files for this processor
        #{generated_config_files |> hd()}: where you find config files for this processor
        #{procon_producer_messages_migration}: store messages to send to kafka (auto increment index for exactly once processing on consumer side)
        #{procon_consumer_indexes_migration}: store topic/partition consumed indexes (for exactly once processing)
        #{procon_producer_balancings_migration}: store which app/container produces which topic/partition
        #{procon_producer_indexes_migration}: store producers indexes for transactional information
        #{processor_entity_migration}: default entity managed by this processor
        #{controller_errors_path}: list of errors for http reponse
        #{controller_helpers_path}: controllers helpers

      IMPORTANT!! To finish the setup:
      --------------------------------

      * add this line to lib/web/#{app_name}_web/router.ex:

        forward "/#{processor_name |> Helpers.short_processor_name()}", #{processor_name}.Web.Router

      * add these lines in ./config/config.exs :

        config :procon,
          brokers: [localhost: 9092],
          brod_client_config: [reconnect_cool_down_seconds: 10],
          nb_simultaneous_messages_to_send: 1000,
          offset_commit_interval_seconds: 5,
          consumers: []
        }

      * add the processor repository #{processor_name |> Helpers.short_processor_name()} to "config/config.exs" in "ecto_repos" array
      * add the processor repository #{processor_name |> Helpers.short_processor_name()} to "lib/[your_app]/application.ex" in children array to start        the repo when the application starts.

      * configure your processor in #{generated_config_files |> tl()}. This is where you add your kafka listeners.

    generate serializers for your resources (data your service is master of and will generate events):
      mix procon.serializer --resource ResourceName


    Now you can produce message :

      Procon.MessagesEnqueuers.Ecto.enqueue_event(event_data, EventSerializerModule, event_type)
      Procon.MessagesProducers.ProducersStarter.start_topic_production(nb_messages_to_batch, ProcessorRepository, topic)

    where :
      - event_data: a map/struct of your data for your message
      - event_type: a state (:created or :updated or :deleted)
      - EventSerializerModule: module serializer you have generated and parameterized
      - nb_messages_to_batch: number of messages to send at the same time (use 1000 to start)
      - ProcessorRepository: the repository module where messages are stored
      - topic: the topic to start production for


    generate messages controller to consume events from kafka:
      mix procon.controller --schema SchemaName

    """

    Helpers.info(msg)
  end

  def create_default_services(services_path, processor_name, processor_repo, crud) do
    services_domain_path = [services_path, "domain"] |> Path.join()
    Helpers.info("creating services domain directory #{services_domain_path}")
    services_domain_path |> create_directory()
    if String.contains?(crud, "c") do
      services_domain_path |> Helpers.DefaultCreator.create_default_create_service(processor_name, processor_repo)
    end

    if String.contains?(crud, "r") || String.contains?(crud, "i") do
      services_domain_path |> Helpers.DefaultGetter.create_default_getter_service(processor_name, processor_repo, crud)
    end

    if String.contains?(crud, "u") do
      services_domain_path |> Helpers.DefaultUpdater.create_default_update_service(processor_name, processor_repo)
    end

    if String.contains?(crud, "d") do
      services_domain_path |> Helpers.DefaultDeleter.create_default_delete_service(processor_name, processor_repo)
    end
  end
end
