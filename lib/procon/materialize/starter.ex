defmodule Procon.Materialize.Starter do
  require Logger

  def run_materialize_configs(processors) do
    Procon.Helpers.olog(
      "🎃 PROCON > MATERIALIZE: starting to configure materialize for activated processors (#{inspect(processors)})",
      Procon.Materialize.StarterRun,
      ansi_color: :blue
    )

    Application.get_env(:procon, Processors)
    |> Procon.Parallel.pmap(
      fn {processor_name, processor_config} ->
        case Enum.empty?(processors) || Enum.any?(processors, &(&1 == processor_name)) do
          true ->
            setup_materialize_for_processor(
              processor_name,
              Keyword.get(processor_config, :materialize, nil)
            )
            |> case do
              :ok ->
                nil

              unregistered_proc_name ->
                unregistered_proc_name
            end

          false ->
            nil
        end
      end,
      30000
    )
    |> Enum.reject(&is_nil/1)
    |> Procon.Helpers.olog(Procon.Materialize.Starter, ansi_color: :blue)
    |> case do
      [] ->
        Procon.Helpers.log(
          "👍 done : all processors registered!",
          ansi_color: :blue
        )

      unregistered_procs ->
        Procon.Helpers.log("All processors not yet registered.", ansi_color: :blue)
        Procon.Helpers.log(unregistered_procs, ansi_color: :blue)

        Procon.Helpers.log("starting another registration with unregistered processors only",
          ansi_color: :blue
        )

        run_materialize_configs(unregistered_procs)
    end
  end

  @spec setup_materialize_for_processor(any, nil | map) ::
          nil
          | :ok
          | atom()
  def setup_materialize_for_processor(processor_name, nil) do
    Procon.Helpers.olog(
      "🎃😑 PROCON > MATERIALIZE: no materialize config for processor #{inspect(processor_name)}.",
      Procon.Materialize.Starter,
      ansi_color: :blue
    )

    :ok
  end

  def setup_materialize_for_processor(processor_name, materialize_processor_config) do
    Procon.Helpers.olog(
      "🎃❎ PROCON > MATERIALIZE: Configure materialize for processor #{inspect(processor_name)}",
      Procon.Materialize.StarterResult,
      ansi_color: :blue
    )

    case :epgsql.connect(
           Map.take(materialize_processor_config, [:database, :host, :port, :username])
         ) do
      {:ok, epgsql_pid} ->
        result =
          Enum.map(
            materialize_processor_config.queries,
            fn query ->
              :epgsql.squery(epgsql_pid, query)
              |> case do
                {:ok, [], []} ->
                  Procon.Helpers.olog(
                    "🎃❎🔧 PROCON > MATERIALIZE > QUERY: executed for processor #{processor_name}",
                    Procon.Materialize.StarterResultOk,
                    ansi_color: :blue
                  )

                  :ok

                {:error, {:error, :error, _reference, :internal_error, error_description, []}} ->
                  Procon.Helpers.olog(
                    [
                      "🎃❌🔧 PROCON > MATERIALIZE > QUERY: unable to execute for processor #{processor_name}",
                      inspect(error_description)
                    ],
                    Procon.Materialize.StarterResultError,
                    ansi_color: :red
                  )

                  nil
              end
            end
          )

        :ok = :epgsql.close(epgsql_pid)

        result
        |> Enum.any?(&is_nil/1)
        |> case do
          true ->
            processor_name

          _ ->
            :ok
        end

      {:error, reason} ->
        Procon.Helpers.olog(
          "🎃❌ PROCON > MATERIALIZE > epgsql.connect error: Unable to configure materialize for processor #{processor_name}",
          Procon.Materialize.Starter,
          ansi_color: :blue
        )

        {:error, reason}
    end
  end
end
