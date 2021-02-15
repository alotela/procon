defmodule Procon.Materialize.Starter do
  use GenServer

  require Logger

  def start_link(options) do
    GenServer.start_link(
      __MODULE__,
      Keyword.get(options, :initial_state, []),
      name: __MODULE__
    )
  end

  ## GenServer callbacks
  def init(initial_state) do
    Process.sleep(5000)
    GenServer.cast(__MODULE__, {:start})
    {:ok, initial_state}
  end

  def handle_cast({:start}, state) do
    run_materialize_configs()

    {:noreply, state}
  end

  def run_materialize_configs() do
    Logger.info("PROCON: starting to configure materialize for activated processors")
    Procon.ProcessorConfigAccessor.activated_processors_config()
    |> Procon.Parallel.pmap(fn {processor_name, processor_config} ->
      setup_materialize_for_processor(
        processor_name,
        Keyword.get(processor_config, :materialize, nil)
      )
    end)
  end

  def setup_materialize_for_processor(processor_name,  nil) do
    Logger.info("PROCON: no materialize config for processor #{processor_name}.")
  end

  def setup_materialize_for_processor(processor_name,  materialize_processor_config) do
    Logger.info("Configure materialize for processor #{processor_name}")
    case :epgsql.connect(
      Map.take(materialize_processor_config, [:database, :host, :port, :username])) do
      {:ok, epgsql_pid} ->
        Enum.map(
        materialize_processor_config.queries,
        fn query -> :epgsql.squery(epgsql_pid, query) |> IO.inspect() end)

        :ok = :epgsql.close(epgsql_pid)

        :ok

      {:error, reason} ->
        Logger.warn("Unable to configure materialize for processor #{processor_name}")
        {:error, reason}
    end
  end
end
