defmodule Mix.Tasks.Procon.Helpers do
  def file_exists?(migrations_path, globe) do
    [] != migrations_path |> Path.join(globe) |> Path.wildcard()
  end

  def processor_path(processor_name) do
    Path.join([
      "lib",
      "processors",
      processor_name |> String.split(".") |> List.last() |> Macro.underscore()
    ])
  end

  def repo_name_to_module(processor_name, processor_repo) do
    Module.concat([processor_name, Repositories, processor_repo])
    |> to_string()
    |> String.replace("Elixir.", "")
  end

  def default_controller_module(processor_name) do
    "#{processor_name}.Web.Controllers.#{processor_name |> processor_to_controller()}"
  end

  def info(msg) do
    msg = """
    #{msg}
    """

    Mix.shell().info([msg])
  end

  def default_serializer_module(processor_name) do
    "#{processor_name}.Events.Serializers.#{
      processor_name |> processor_to_controller() |> Inflex.singularize()
    }"
  end

  def default_service_name(processor_name) do
    "#{processor_name}.Services.Domain.#{processor_name |> processor_to_controller()}"
  end

  def default_schema_module(processor_name) do
    processor_name
    |> Kernel.<>(".Schemas.")
    |> Kernel.<>(
      processor_name
      |> String.split(".")
      |> List.last()
      |> Inflex.singularize()
    )
  end

  def processor_to_controller(processor_name) do
    processor_name
    |> String.split(".")
    |> List.last()
  end

  def processor_to_kebab_resource(processor_name) do
    processor_name
    |> processor_to_resource()
    |> String.replace("_", "-")
  end

  def processor_to_resource(processor_name) do
    processor_name
    |> String.split(".")
    |> List.last()
    |> Macro.underscore()
  end

  def short_processor_name(processor_name) do
    processor_name |> String.split(".") |> List.last() |> Macro.underscore()
  end
end
