defmodule Mix.Tasks.Procon.Init do
  use Mix.Task
  import Mix.Generator

  @shortdoc "Initialize Procon in your project"

  def run(_args) do
    app_name = Mix.Project.config()[:app] |> to_string
    host_app_main_repo = Mix.Ecto.parse_repo([]) |> List.first()

    migrations_path =
      Path.join(
        "priv/#{host_app_main_repo |> Module.split() |> List.last() |> Macro.underscore()}",
        "migrations"
      )

    create_directory(migrations_path)

    procon_producer_messages_migration =
      generate_migration(
        "procon_producer_messages",
        migrations_path,
        host_app_main_repo,
        &procon_producer_messages_template/1
      )

    procon_consumer_indexes_migration =
      generate_migration(
        "procon_consumer_indexes",
        migrations_path,
        host_app_main_repo,
        &procon_consumer_indexes_template/1
      )

    procon_producer_balancings_migration =
      generate_migration(
        "procon_producer_balancings",
        migrations_path,
        host_app_main_repo,
        &procon_producer_balancings_template/1
      )

    procon_producer_indexes_migration =
      generate_migration(
        "procon_producer_indexes",
        migrations_path,
        host_app_main_repo,
        &procon_producer_indexes_template/1
      )

    create_directory(Path.join(["lib", "events_serializers"]))
    create_directory(Path.join(["lib", app_name, "message_controllers"]))

    msg = """
    Procon initialized for your project.

    Generated files and directories:
        #{procon_producer_messages_migration}: store messages to send to kafka (auto increment index for exactly once processing on consumer side)
        #{procon_consumer_indexes_migration}: store topic/partition consumed indexes (for exactly once processing)
        #{procon_producer_balancings_migration}: store which app/container produces which topic/partition
        #{procon_producer_indexes_migration}: store producers indexes for transactional information
        lib/events_serializers: directory where your events serializers will be generated
        lib/#{app_name}/message_controllers: directory where your messages controllers will be generated

    To finish setup, add these lines in ./config/config.exs:
          config :procon,
          brokers: [localhost: 9092],
          consumer_group_name: "",
          default_realtime_topic: "refresh_events",
          brod_client_config: [reconnect_cool_down_seconds: 10],
          broker_client_name: :kafka_client_service,
          messages_repository: #{inspect(host_app_main_repo)},
          nb_simultaneous_messages_to_send: 1000,
          offset_commit_interval_seconds: 5,
          routes: %{
            "service/resource/created" => {"topic_name", "body_version", Module, :function}
          },
          service_name: "#{app_name}", # use to build messages events name "service_name/resource/state"


    generate serializers for your resources (data your service is master of and will generate events):
        mix procon.serializer --resource ResourceName


    Now you can produce message:
        Procon.MessagesEnqueuers.Ecto.enqueue_event(event_data, event_serializer, event_status)
    where:
        - event_data: map of your data for your message
        - event_status: :created or :updated or :deleted
        - event_serializer: module serializer you have generated and parameterized


    generate messages controller to consume events from kafka:
        mix procon.controller --schema SchemaName

    """

    Mix.shell().info([msg])
  end

  defp generate_migration(filename, migrations_path, host_app_main_repo, template_function) do
    unless file_exists?(migrations_path, "*_#{filename}.exs") do
      file = Path.join(migrations_path, "#{timestamp()}_#{filename}.exs")
      create_file(file, template_function.(host_app_main_repo: host_app_main_repo))
      file
    end
  end

  defp timestamp do
    :timer.sleep(1000)
    {{y, m, d}, {hh, mm, ss}} = :calendar.universal_time()
    "#{y}#{pad(m)}#{pad(d)}#{pad(hh)}#{pad(mm)}#{pad(ss)}"
  end

  defp pad(i) when i < 10, do: <<?0, ?0 + i>>
  defp pad(i), do: to_string(i)

  embed_template(:procon_producer_messages, """
  defmodule <%= inspect @host_app_main_repo %>.Migrations.ProconProducerMessages do
    use Ecto.Migration

    def change do
      create table(:procon_producer_messages) do
        add :blob, :text, null: false
        add :is_stopped, :boolean
        add :partition, :integer, null: false
        add :stopped_error, :text
        add :stopped_message_id, :integer
        add :topic, :string, null: false
        timestamps
      end
      create index(:procon_producer_messages, [:is_stopped])
      create index(:procon_producer_messages, [:partition])
      create index(:procon_producer_messages, [:topic])
      alter table(:procon_producer_messages) do
        modify :id, :int8
      end
    end
  end
  """)

  embed_template(:procon_consumer_indexes, """
  defmodule <%= inspect @host_app_main_repo %>.Migrations.ProconMessageIndexes do
    use Ecto.Migration

    def change do
      create table(:procon_consumer_indexes) do
        add :message_id, :int8, null: false
        add :partition, :integer, null: false
        add :topic, :string, null: false
        add :error, :text, null: false
        timestamps
      end
      create index(:procon_consumer_indexes, [:from])
      create index(:procon_consumer_indexes, [:partition])
      create index(:procon_consumer_indexes, [:topic])
    end
  end
  """)

  embed_template(:procon_producer_indexes, """
  defmodule <%= inspect @host_app_main_repo %>.Migrations.ProconProducerIndexes do
    use Ecto.Migration

    def change do
      create table(:procon_producer_indexes) do
        add :last_index, :int8, null: false
        add :partition, :integer, null: false
        add :topic, :string, null: false
      end
      create index(:procon_producer_indexes, [:partition])
      create index(:procon_producer_indexes, [:topic])
    end
  end
  """)

  embed_template(:procon_enqueur, """
  defmodule <%= @app_module %>.Procon.Enqueur do
    # this module is just an 'alias' to the real module
    # here we use Ecto, but you can use any other compatible strategy
    import Procon.MessagesEnqueuers.Ecto
  end

  """)

  embed_template(:procon_producer_balancings, """
  defmodule <%= inspect @host_app_main_repo %>.Migrations.ProconProducerBalancings do
    use Ecto.Migration

    def change do
      create table(:procon_producer_balancings) do
        add :id_producer, :integer
        add :topic, :string
        add :partition, :integer
        add :last_presence_at, :utc_datetime
      end
      create index(:procon_producer_balancings, [:partition])
      create index(:procon_producer_balancings, [:topic])
    end
  end
  """)

  defp file_exists?(migrations_path, globe) do
    [] != migrations_path |> Path.join(globe) |> Path.wildcard()
  end
end
