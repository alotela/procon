defmodule Mix.Tasks.Procon.Helpers do
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

  def info(msg) do
    msg = """
    #{msg}
    """

    Mix.shell().info([msg])
  end
end
