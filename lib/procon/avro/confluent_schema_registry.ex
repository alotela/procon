defmodule Procon.Avro.ConfluentSchemaRegistry do
  require Logger

  def register_all_avro_schemas() do
    Logger.info("Registering all avro schema in confluent schema registry")
    do_registration()
  end

  def do_registration() do
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
    end)
  end

  def register_schema(filename) do
    schema_ref = filename |> String.replace(".avsc", "")

    Logger.info(
      "🎃🍾 Procon.Avro.ConfluentSchemaRegistry > register_schema : schema #{schema_ref} loading from file..."
    )

    {:ok, schema} = Avrora.Storage.File.get(schema_ref)

    Logger.info(
      "🎃🍾🍾 Procon.Avro.ConfluentSchemaRegistry > register_schema : schema #{schema_ref} loaded from file."
    )

    [_, schema_subject] = String.split(schema_ref, ".")

    Avrora.Utils.Registrar.register_schema(schema, as: schema_subject)
    |> case do
      {:error, :conflict} ->
        Logger.info(
          "🎃🍾🍾🍾 Procon.Avro.ConfluentSchemaRegistry > register_schema : schema #{schema_ref} already registered in confluent schema registry as #{
            schema_subject
          }."
        )

        :ok

      {:ok, _schema_with_id} ->
        Logger.info(
          "🎃🍾🍾🍾 Procon.Avro.ConfluentSchemaRegistry > register_schema : schema #{schema_ref} registered in confluent schema registry as #{
            schema_subject
          }."
        )

        :ok
    end
  end
end