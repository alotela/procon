defmodule Mix.Tasks.Procon.Helpers.Migrations do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def generate_migration(
        timestamp,
        filename,
        migrations_path,
        processor_name,
        processor_repo,
        template
      ) do
    file = Path.join(migrations_path, "#{timestamp}_#{filename}.exs")

    unless Helpers.file_exists?(migrations_path, "*_#{filename}.exs") do
      args = [
        processor_repo: Helpers.repo_name_to_module(processor_name, processor_repo),
        processor_short_name: Helpers.processor_to_controller(processor_name),
        table: Helpers.processor_to_resource(processor_name)
      ]

      content =
        case template do
          :procon_dynamic_topics ->
            procon_dynamic_topics_template(args)

          :procon_consumer_indexes ->
            procon_consumer_indexes_template(args)

          :procon_enqueur ->
            procon_enqueur_template(args)

          :procon_producer_balancings ->
            procon_producer_balancings_template(args)

          :processor_entity ->
            processor_entity_template(args)

          :procon_realtimes ->
            procon_realtimes_template(args)
        end

      create_file(file, content)
    end

    file
  end

  def timestamp(seed \\ nil) do
    case seed do
      nil ->
        {{y, m, d}, {hh, mm, ss}} = :calendar.universal_time()
        "#{y}#{pad(m)}#{pad(d)}#{pad(hh)}#{pad(mm)}#{pad(ss)}" |> String.to_integer()

      time ->
        time + 1
    end
  end

  defp pad(i) when i < 10, do: <<?0, ?0 + i>>
  defp pad(i), do: to_string(i)

  embed_template(:procon_consumer_indexes, """
  defmodule <%= @processor_repo %>.Migrations.ProconMessageIndexes do
    use Ecto.Migration

    def change do
      create table(:procon_consumer_indexes) do
        add(:message_id, :int8, null: false)
        add(:partition, :integer, null: false)
        add(:topic, :string, null: false)
        add(:error, :text, null: true)
        timestamps()
      end
      create index(:procon_consumer_indexes, [:partition])
      create index(:procon_consumer_indexes, [:topic])
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
  defmodule <%= @processor_repo %>.Migrations.ProconProducerBalancings do
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

  embed_template(:processor_entity, """
  defmodule <%= @processor_repo %>.Migrations.Add<%= @processor_short_name %>Table do
    use Ecto.Migration

    def change do
      create table(:<%= @table %>, primary_key: false) do
        add(:id, :uuid, primary_key: true)
        add(:metadata, :map, null: false, default: %{})
      end

      execute("ALTER TABLE @table REPLICA IDENTITY FULL")
    end
  end
  """)

  embed_template(:procon_realtimes, """
  defmodule <%= @processor_repo %>.Migrations.AddProconRealtimesTable do
    use Ecto.Migration

    def change do
      create table(:<%= @table %>) do
        add(:session_id, :string)
        add(:channel, :string)
        add(:metadata, :map, null: false, default: %{})
      end

      execute("ALTER TABLE @table REPLICA IDENTITY FULL")
    end
  end
  """)

  embed_template(:procon_dynamic_topics, """
  defmodule <%= @processor_repo %>.Migrations.AddTableProconDynamicTopics do
    use Ecto.Migration

    def change do
      create table(:procon_dynamic_topics) do
        add(:entity, :string)
        add(:inserted_at, :naive_datetime)
        add(:partitions_count, :integer)
        add(:processor, :string)
        add(:tenant_id, :binary_id)
        add(:topic_name, :string)
      end

      create(unique_index(:procon_dynamic_topics, [:topic_name], name: :unique_topic))

      execute("ALTER TABLE procon_dynamic_topics REPLICA IDENTITY FULL")
    end
  end
  """)
end
