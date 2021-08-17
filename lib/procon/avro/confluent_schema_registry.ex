defmodule Procon.Avro.ConfluentSchemaRegistry do
  require Logger

  def register_all_avro_schemas() do
    Procon.Helpers.olog("Registering all avro schema in confluent schema registry",
      Procon.Avro.ConfluentSchemaRegistry
    )
    copy_procon_avro_schemas_to_priv_dir()
    do_registration()
  end

  def copy_procon_avro_schemas_to_priv_dir() do
    Application.app_dir(:procon, ["priv", "schemas"])
    |> File.cp_r!(
      Path.join([File.cwd!(), "priv", "schemas"])
    )
  end

  def do_registration() do
    {:ok, generic_key_schema} = "procon-generic-key" |> Avrora.Storage.File.get()
    [
      "priv",
      "schemas",
      "**",
      "*.avsc"
    ]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.map(fn <<"priv/schemas/", rest::binary>> ->
      String.replace(rest, "/", ".")
      |> register_schema()
      |> register_key_schema(generic_key_schema)
    end)
  end

  def register_schema(filename) do
    schema_ref = filename |> String.replace(".avsc", "")

    Procon.Helpers.olog(
      "🎃🍾 Procon.Avro.ConfluentSchemaRegistry > register_schema : schema #{schema_ref} loading from file...",
      Procon.Avro.ConfluentSchemaRegistry
    )

    {:ok, schema} = Avrora.Storage.File.get(schema_ref)

    Procon.Helpers.olog(
      "🎃🍾🍾 Procon.Avro.ConfluentSchemaRegistry > register_schema : schema #{schema_ref} loaded from file.",
      Procon.Avro.ConfluentSchemaRegistry
    )

    Avrora.Utils.Registrar.register_schema(schema, as: schema_ref)
    |> case do
      {:error, :conflict} ->
        Procon.Helpers.olog(
          "🎃🍾🍾🍾 Procon.Avro.ConfluentSchemaRegistry > register_schema : schema #{schema_ref} already registered in confluent schema registry as #{
            schema_ref
          }.",
      Procon.Avro.ConfluentSchemaRegistry
    )

        :ok

      {:ok, schema_with_id} ->
        Procon.Helpers.olog(
          "🎃🍾🍾🍾 Procon.Avro.ConfluentSchemaRegistry > register_schema : schema #{schema_ref} registered in confluent schema registry as #{
            schema_ref
          }.",
      Procon.Avro.ConfluentSchemaRegistry
    )

        {schema_ref, schema_with_id}
    end
  end

  def register_key_schema({schema_ref, schema_with_id}, generic_key_schema) do
    Avrora.Utils.Registrar.register_schema(
      generic_key_schema |> Map.put(:full_name, schema_with_id.full_name <> "Key"),
      as: schema_ref |> String.replace_suffix("-value", "-key")
    )
    |> case do
      {:error, :conflict} ->
        Procon.Helpers.olog(
          "🎃🍾🍾🍾 Procon.Avro.ConfluentSchemaRegistry > register_key_schema : schema #{schema_ref}-key already registered in confluent schema registry as #{
            schema_ref
          }.",
      Procon.Avro.ConfluentSchemaRegistry
    )

        :ok

      {:ok, _schema_with_id} ->
        Procon.Helpers.olog(
          "🎃🍾🍾🍾 Procon.Avro.ConfluentSchemaRegistry > register_schema : schema #{schema_ref}-key registered in confluent schema registry as #{
            schema_ref
          }.",
      Procon.Avro.ConfluentSchemaRegistry
    )

        :ok
    end
  end

  def topic_to_avro_value_schema(topic), do: "#{topic}-value"
  def topic_to_avro_key_schema(topic), do: "#{topic}-key"
end
