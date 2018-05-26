defmodule Mix.Tasks.Procon.CreateTables do
  use Mix.Task
  import Mix.Generator

  @shortdoc "Create migration files for Ecto"

  def run(_args) do
    app_name = Mix.Project.config[:app] |> to_string
    app_module = app_name |> Macro.camelize
    host_app_main_repo = Mix.Ecto.parse_repo([]) |> List.first

    migrations_path = Path.join("priv/#{host_app_main_repo |> Module.split |> List.last |> Macro.underscore}", "migrations")

    create_directory(migrations_path)

    generate_migration(
      "procon_producer_messages", 
      migrations_path, 
      host_app_main_repo,
      &procon_producer_messages_template/1
    )

    generate_migration(
      "procon_consumer_indexes", 
      migrations_path, 
      host_app_main_repo,
      &procon_consumer_indexes_template/1
    )

    generate_migration(
      "procon_producer_balancings_template", 
      migrations_path, 
      host_app_main_repo,
      &procon_producer_balancings_template/1
    )

    default_enqueur_path = Path.join(["lib", "procon"])
    create_directory(default_enqueur_path)

    unless file_exists?(default_enqueur_path, "*enqueur.ex") do
      default_enqueur_path
      |> Path.join("enqueur.ex")
      |> create_file(procon_enqueur_template([app_module: app_module, app_name: app_name, repo: host_app_main_repo]))
    end
    create_directory(Path.join ["lib", "message_serializers"] )
    create_directory(Path.join ["lib", "message_controllers"] )

    msg = "
Procon tables created.

All files generated. To finish setup, add this line to your config file:
    config :procon, producer: [handler: #{app_module}.Procon.Enqueur, frequency: 100, number_of_messages: 1000]
    config :procon, repository: #{inspect host_app_main_repo}

lib/procon/enqueur.ex is a default message enqueur generated for you (YOU NEED TO PARAMETER THIS!!!!! DON'T FORGET TO LOOK AT IT!)
generate serializers for your ecto schema:
    mix procon.serializer --schema MyAppEctoSchema
and produce messages!

generate messages controller to consume events from kafka:
    mix procon.controller --schema MyEctoSchemaForAnEvent

"
    Mix.shell.info [msg]
  end

  defp generate_migration(filename, migrations_path, host_app_main_repo, template_function) do
    unless file_exists?(migrations_path, "*#{filename}_#{filename}.es") do
      file = Path.join(migrations_path, "#{timestamp()}_#{filename}.exs")
      create_file file, template_function.([host_app_main_repo: host_app_main_repo])
    end
  end

  defp timestamp do
    {{y, m, d}, {hh, mm, ss}} = :calendar.universal_time()
    "#{y}#{pad(m)}#{pad(d)}#{pad(hh)}#{pad(mm)}#{pad(ss)}"
  end

  defp pad(i) when i < 10, do: << ?0, ?0 + i >>
  defp pad(i), do: to_string(i)

  embed_template :procon_producer_messages, """
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
  """

  embed_template :procon_consumer_indexes, """
  defmodule <%= inspect @host_app_main_repo %>.Migrations.ProconMessageIndexes do
    use Ecto.Migration

    def change do
      create table(:procon_consumer_indexes) do
        add :from, :string, null: false
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
  """

  embed_template :procon_enqueur, """
  defmodule <%= @app_module %>.Procon.Enqueur do
    @application :<%= @app_name %> #should be your app name
    @producer_name "<%= @app_name %>" #'from' field in messages
    @realtime_topic "" #don't leave this blank!
    @messages_topic "" #don't leave this blank!
    @datastore <%= inspect @repo %> #store module for messages
    @serializers <%= @app_module %>.MessageSerializers #your serializers module "namespace"
    use Procon.MessagesEnqueuers.Ecto
  end

  """

  embed_template :procon_producer_balancings, """
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
  """

  defp file_exists?(migrations_path, globe) do
    [] != migrations_path |> Path.join(globe) |> Path.wildcard
  end
end
