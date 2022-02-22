defmodule Procon.Avro.Starter do
  require Logger

  def register_schemas(processors) do
    {:ok, _} = Application.ensure_all_started(:avrora)
    {:ok, _} = Elixir.Avrora.start_link()

    copy_procon_avro_schemas_to_priv_dir()

    case processors do
      [] ->
        [Elixir.Avrora.Config.self().schemas_path(), "**", "*.avsc"]
        |> Path.join()
        |> Path.wildcard()
        |> Enum.each(fn file_path ->
          file_path
          |> Path.relative_to(Elixir.Avrora.Config.self().schemas_path())
          |> Path.rootname()
          |> String.replace("/", ".")
          |> register_schema()
        end)

      names ->
        names
        |> Enum.each(fn name ->
          name
          |> String.trim()
          |> String.replace(".", "_")
          |> register_schema()
        end)
    end
  end

  def copy_procon_avro_schemas_to_priv_dir() do
    Application.app_dir(:procon, ["priv", "schemas"])
    |> File.cp_r!(Path.join([File.cwd!(), "priv", "schemas"]))
  end

  defp register_schema(schema_name) do
    register_schema_by_name(schema_name, as: String.replace(schema_name, ".", "-"))

    register_schema_by_name("procon-generic-key",
      as: String.replace(schema_name, ".", "-") |> String.replace_suffix("value", "key")
    )
  end

  defp register_schema_by_name(name, opts) do
    opts = Keyword.merge(opts, force: true)

    case Elixir.Avrora.Utils.Registrar.register_schema_by_name(name, opts) do
      {:ok, _} ->
        case Keyword.get(opts, :as) do
          nil ->
            Procon.Helpers.olog("schema `#{name}' will be registered", Procon.Avro.Starter)

          new_name ->
            Procon.Helpers.olog(
              "schema `#{name}' will be registered as `#{new_name}'",
              Procon.Avro.Starter
            )
        end

      {:error, error} ->
        Procon.Helpers.olog(
          [
            "schema `#{name}' will be skipped due to an error",
            inspect(error)
          ],
          Procon.Avro.Starter,
          ansi_color: :red
        )
    end
  end
end
