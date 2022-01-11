defmodule Mix.Tasks.Procon.Truncate.Tables do
  use Mix.Task

  @shortdoc "Truncate tables of all databases"

  alias Mix.Tasks

  @cli_options [
    strict: [
      all: :boolean,
      processor: :string
    ]
  ]

  @impl Mix.Task
  def run(argv) do
    Mix.shell().info("starting truncate tables...")

    Tasks.Loadpaths.run(["--no-elixir-version-check", "--no-archives-check"])

    {_opts, _, _} = OptionParser.parse(argv, @cli_options)
    # do not start ecto
    Application.put_env(:procon, :activated_processors, [])
    # do not start kafka producers and consumers
    Application.put_env(:procon, :autostart, false)
    # ensure parent's app config is loaded
    # https://groups.google.com/g/elixir-lang-talk/c/JikyDaoGP9k
    Mix.Task.run("app.start")

    Mix.Shell.IO.info("stopping PG..")
    System.shell("ps aux | grep \"[b]in/postgres\"") |> IO.inspect()

    System.shell("ps aux | grep \"[b]in/postgres\" | awk '{print $2}'")
    # |> Mix.Shell.IO.info()

    System.shell("ps aux | grep \"[b]in/postgres\" | awk '{print $2}' | xargs kill -2")
    # |> Mix.Shell.IO.info()

    # Mix.Shell.IO.shell("ps aux | grep \"[b]in/postgres\" | awk '{print $2}' | xargs kill -2")
    Mix.Shell.IO.info("PG stopped!")

    System.shell(
      "/Applications/Postgres.app/Contents/Versions/14/bin/postgres -D /Users/ceissen/Library/Application\\ Support/Postgres/var-14 -p 5432 &"
    )

    # |> Mix.Shell.IO.info()

    # Mix.Shell.IO.info("stopping PG..")
    # Mix.Shell.IO.cmd("ps aux | grep \"[b]in/postgres\"")
    # Mix.Shell.IO.cmd("ps aux | grep \"[b]in/postgres\" | awk '{print $2}' | xargs kill -2")
    # Mix.Shell.IO.info("PG stopped!")
    #
    # Mix.Shell.IO.info("stopping schema registry..")
    # Mix.Shell.IO.cmd("ps aux | grep \"[s]chema-registry\"")
    # Mix.Shell.IO.cmd("ps aux | grep \"[s]chema-registry\" | awk '{print $2}' | xargs kill -2")
    # Mix.Shell.IO.info("schema registry stopped!")
    #
    # Mix.Shell.IO.info("stopping kafka..")
    # Mix.Shell.IO.cmd("ps aux | grep \"[k]afkaServer\"")
    # Mix.Shell.IO.cmd("ps aux | grep \"[k]afkaServer\" | awk '{print $2}' | xargs kill -2")
    # Mix.Shell.IO.info("kafka stopped!")
    #
    # Mix.Shell.IO.info("stopping zookeeper..")
    # Mix.Shell.IO.cmd("ps aux | grep \"[z]ookeeper\"")
    # Mix.Shell.IO.cmd("ps aux | grep \"[z]ookeeper\" | awk '{print $2}' | xargs kill -2")
    # Mix.Shell.IO.info("zookeeper stopped!")
    #
    # Mix.Shell.IO.info("reseting kafka...")
    #
    # {microseconds_kafka, _} =
    #  :timer.tc(fn ->
    #    Mix.Shell.IO.cmd(
    #      "rm -R /Users/ceissen/apache/confluent-6.0.1/data && mv /Users/ceissen/apache/confluent-6.0.1/data_2 /Users/ceissen/apache/confluent-6.0.1/data"
    #    )
    #  end)
    #
    # Mix.Shell.IO.info("time to reset kafka: #{div(microseconds_kafka, 1_000_000)}")
    #
    # Mix.Shell.IO.info("reseting materialize...")
    #
    # {microseconds_materialize, _} =
    #  :timer.tc(fn ->
    #    Mix.Shell.IO.cmd(
    #      "rm -R  /Users/ceissen/labs/papayecube/materialize/mzdata && cp -R /Users/ceissen/labs/papayecube/materialize/mzdata_2 /Users/ceissen/labs/papayecube/materialize/mzdata"
    #    )
    #  end)
    #
    # Mix.Shell.IO.info("time to reset materialize: #{div(microseconds_materialize, 1_000_000)}")
    #
    # Mix.Shell.IO.info("reseting database (rm)...")
    #
    # {microseconds_database1, _} =
    #  :timer.tc(fn ->
    #    Mix.Shell.IO.cmd("rm -R /Users/ceissen/Library/Application\\ Support/Postgres/var-14")
    #  end)
    #
    # Mix.Shell.IO.info("time to delete database: #{div(microseconds_database1, 1_000_000)}")
    #
    # Mix.Shell.IO.info("reseting database (mv)...")
    #
    # {microseconds_database2, _} =
    #  :timer.tc(fn ->
    #    Mix.Shell.IO.cmd(
    #      "mv /Users/ceissen/Library/Application\\ Support/Postgres/var-14_2 /Users/ceissen/Library/Application\\ Support/Postgres/var-14"
    #    )
    #  end)
    #
    # Mix.Shell.IO.info("time to reset database: #{div(microseconds_database2, 1_000_000)}")
    #
    # Mix.Shell.IO.info("starting PG..")
    #
    # Mix.Shell.IO.cmd(
    #  "/Applications/Postgres.app/Contents/Versions/14/bin/postgres -D /Users/ceissen/Library/Application\\ Support/Postgres/var-14 -p 5432 &"
    # )
    #
    # Mix.Shell.IO.info("PG stopped!")
    #
    # Mix.Shell.IO.info("starting zookeeper..")
    #
    # Mix.Shell.IO.cmd(
    #  "/Users/ceissen/apache/confluent-6.0.1/bin/zookeeper-server-start /Users/ceissen/apache/confluent-6.0.1/etc/kafka/zookeeper.properties &"
    # )
    #
    # Mix.Shell.IO.info("zookeeper started!")
    #
    # Mix.Shell.IO.info("starting kafka..")
    #
    # Mix.Shell.IO.cmd(
    #  "/Users/ceissen/apache/confluent-6.0.1/bin/kafka-server-start /Users/ceissen/apache/confluent-6.0.1/etc/kafka/server.properties &"
    # )
    #
    # Mix.Shell.IO.info("kafka started!")
    #
    # Mix.Shell.IO.info("starting schema registry..")
    #
    # Mix.Shell.IO.cmd(
    #  "/Users/ceissen/apache/confluent-6.0.1/bin/schema-registry-start -daemon /Users/ceissen/apache/confluent-6.0.1/etc/schema-registry/schema-registry.properties &"
    # )
    #
    # Mix.Shell.IO.info("Schema registry started")
    #
    # Mix.Shell.IO.info("copying kafka...")
    #
    # {microseconds_kafka_copy, _} =
    #  :timer.tc(fn ->
    #    Mix.Shell.IO.cmd(
    #      "cp -R /Users/ceissen/apache/confluent-6.0.1/data_ /Users/ceissen/apache/confluent-6.0.1/data_2"
    #    )
    #  end)
    #
    # Mix.Shell.IO.info("time to copy kafka: #{div(microseconds_kafka_copy, 1_000_000)}")
    #
    # Mix.Shell.IO.info("copying materialize...")
    #
    # {microseconds_materialize_copy, _} =
    #  :timer.tc(fn ->
    #    Mix.Shell.IO.cmd(
    #      "cp -R /Users/ceissen/labs/papayecube/materialize/mzdata_ /Users/ceissen/labs/papayecube/materialize/mzdata_2"
    #    )
    #  end)
    #
    # Mix.Shell.IO.info(
    #  "time to copy materialize: #{div(microseconds_materialize_copy, 1_000_000)}"
    # )
    #
    # Mix.Shell.IO.info("copying database...")
    #
    # {microseconds_database_copy, _} =
    #  :timer.tc(fn ->
    #    Mix.Shell.IO.cmd(
    #      "cp -R /Users/ceissen/Library/Application\\ Support/Postgres/var-14_ /Users/ceissen/Library/Application\\ Support/Postgres/var-14_2"
    #    )
    #  end)
    #
    # Mix.Shell.IO.info("time to copy database: #{div(microseconds_database_copy, 1_000_000)}")
    #
    # Mix.Shell.IO.info(
    #  "total reset time: #{div(microseconds_kafka + microseconds_materialize + microseconds_database1 + microseconds_database2 + microseconds_kafka_copy + microseconds_materialize_copy + microseconds_database_copy, 1_000_000)}"
    # )

    # case opts |> Keyword.keys() |> Enum.sort() do
    #  [:all] ->
    #    Application.get_env(:procon, Processors)
    #    |> Procon.Parallel.pmap(
    #      fn {processor_name, processor_config} ->
    #        Keyword.get(processor_config, :consumers)
    #        |> case do
    #          [] ->
    #            Keyword.get(processor_config, :producers)
    #            |> case do
    #              [] ->
    #                Procon.Helpers.log(
    #                  "😱❌ unable to truncate tables for processor #{processor_name}!",
    #                  ansi_color: :blue
    #                )
    #            end
    #        end
    #      end,
    #      30000
    #    )
    #
    #    Procon.Helpers.log(
    #      "👍 done : all processors tables truncated!",
    #      ansi_color: :blue
    #    )
    #
    #  [:processor] ->
    #    Module.concat(
    #      Elixir,
    #      opts[:processor] |> String.trim()
    #    )
    #
    #  _ ->
    #    message = """
    #    don't know how to handle `#{Enum.join(argv, " ")}'
    #    """
    #
    #    Mix.shell().error(message)
    #    exit({:shutdown, 1})
    # end
  end
end
