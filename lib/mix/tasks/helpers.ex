defmodule Mix.Tasks.Procon.Helpers do
  def file_exists?(migrations_path, globe) do
    [] != migrations_path |> Path.join(globe) |> Path.wildcard()
  end

  def processor_path(processor_name) do
    Path.join([
      "lib",
      "processors",
      processor_type(processor_name) |> Macro.underscore(),
      short_processor_name(processor_name)
    ])
  end

  def processor_type(processor_name) do
    processor_name
    |> String.split(".")
    |> List.pop_at(2)
    |> elem(0)
  end

  def router_file_path(processor_name) do
    [processor_path(processor_name), "web", "router.ex"] |> Path.join()
  end

  def migrations_path(processor_name, processor_repo) do
    Path.join([
      "priv",
      processor_type(processor_name) |> Macro.underscore(),
      processor_repo |> Macro.underscore(),
      "migrations"
    ])
  end

  def config_directory(processor_name) do
    Path.join([
      "config",
      "processors",
      processor_type(processor_name) |> Macro.underscore(),
      short_processor_name(processor_name)
    ])
  end

  def repo_name_to_module(processor_name, processor_repo) do
    Module.concat([processor_name, Repositories, processor_repo])
    |> to_string()
    |> String.replace("Elixir.", "")
  end

  def default_controller_module(processor_name, public_controller) do
    "#{processor_name}.Web.Controllers.#{if public_controller, do: "", else: "Private."}#{processor_name |> processor_to_controller()}"
  end

  def info(msg) do
    msg = """
    #{msg}
    """

    Mix.Shell.IO.info([msg])
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
