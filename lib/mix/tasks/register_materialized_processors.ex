defmodule Mix.Tasks.Procon.Reg.Materialized do
  use Mix.Task

  @shortdoc "Register materialize processors in Materialize"

  alias Mix.Tasks

  @cli_options [
    strict: [
      all: :boolean,
      processor: :string
    ]
  ]

  @impl Mix.Task
  def run(argv) do
    Mix.shell().info("starting materialize processors requests...")

    Tasks.Loadpaths.run(["--no-elixir-version-check", "--no-archives-check"])

    {opts, _, _} = OptionParser.parse(argv, @cli_options)

    # do not start ecto
    Application.put_env(:procon, :activated_processors, [])
    # do not start kafka producers and consumers
    Application.put_env(:procon, :autostart, false)
    # ensure parent's app config is loaded
    # https://groups.google.com/g/elixir-lang-talk/c/JikyDaoGP9k
    Mix.Task.run("app.start")

    case opts |> Keyword.keys() |> Enum.sort() do
      [:all] ->
        Procon.Materialize.Starter.run_materialize_configs([])

      [:processor] ->
        Module.concat(
          Elixir,
          opts[:processor] |> String.trim()
        )
        |> Procon.Materialize.Starter.run_materialize_configs()

      _ ->
        message = """
        don't know how to handle `#{Enum.join(argv, " ")}'
        """

        Mix.shell().error(message)
        exit({:shutdown, 1})
    end
  end
end
