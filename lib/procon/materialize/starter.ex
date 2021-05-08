defmodule Procon.Materialize.Starter do
  use GenServer

  require Logger

  def start_link(options) do
    Procon.Avro.ConfluentSchemaRegistry.register_all_avro_schemas()

    GenServer.start_link(
      __MODULE__,
      Keyword.get(options, :initial_state, []),
      name: __MODULE__
    )
  end

  ## GenServer callbacks
  def init(initial_state) do
    GenServer.cast(__MODULE__, {:start})
    {:ok, initial_state}
  end

  def handle_cast({:start}, state) do
    run_materialize_configs()

    {:noreply, state}
  end

  def run_materialize_configs() do
    Logger.info(
      "🎃 PROCON > MATERIALIZE: starting to configure materialize for activated processors",
      ansi_color: :blue
    )

    Procon.ProcessorConfigAccessor.activated_processors_config()
    |> Procon.Parallel.pmap(fn {processor_name, processor_config} ->
      setup_materialize_for_processor(
        processor_name,
        Keyword.get(processor_config, :materialize, nil)
      )
    end)
  end

  def setup_materialize_for_processor(processor_name, nil) do
    Logger.info("🎃😑 PROCON > MATERIALIZE: no materialize config for processor #{processor_name}.",
      ansi_color: :blue
    )
  end

  def setup_materialize_for_processor(processor_name, materialize_processor_config) do
    Logger.info("🎃❎ PROCON > MATERIALIZE: Configure materialize for processor #{processor_name}",
      ansi_color: :blue
    )

    case :epgsql.connect(
           Map.take(materialize_processor_config, [:database, :host, :port, :username])
         ) do
      {:ok, epgsql_pid} ->
        Enum.map(
          materialize_processor_config.queries,
          fn query ->
            :epgsql.squery(epgsql_pid, query)
            |> case do
              {:ok, [], []} ->
                Logger.info(
                  "🎃❎🔧 PROCON > MATERIALIZE > QUERY: #{query} executed for processor #{
                    processor_name
                  }"
                )

              {:error, {:error, :error, _reference, :internal_error, error_description, []}} ->
                Logger.info([
                  "🎃❌🔧 PROCON > MATERIALIZE > QUERY: unable to execute #{query} for processor #{
                    processor_name
                  }",
                  inspect(error_description)
                ])
            end
          end
        )

        :ok = :epgsql.close(epgsql_pid)

        :ok

      {:error, reason} ->
        Logger.warn(
          "🎃❌ PROCON > MATERIALIZE > epgsql.connect error: Unable to configure materialize for processor #{
            processor_name
          }",
          ansi_color: :blue
        )

        {:error, reason}
    end
  end
end
