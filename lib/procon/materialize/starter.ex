defmodule Procon.Materialize.Starter do
  require Logger

  def get_runtime_config_brokers_hosts() do
    Application.fetch_env!(:procon, :brokers)
    |> Enum.map(fn {url, port} ->
      "#{url}:#{port}"
    end)
    |> Enum.join(", ")
  end

  def get_runtime_config_materialize() do
    Application.fetch_env!(:procon, :materialize)
    |> Enum.map(fn {key, value} ->
      case key do
        :host ->
          if is_binary(value) do
            {key, to_charlist(value)}
          else
            {key, value}
          end

        :port ->
          if is_binary(value) do
            {key, Integer.parse(value) |> elem(0)}
          else
            {key, value}
          end

        _ ->
          {key, value}
      end
    end)
    |> Enum.into(%{})
  end

  def get_runtime_config_registry_url() do
    Application.fetch_env!(:procon, :registry_url)
  end

  def query_types(),
    do: [
      {~r/^CREATE[[:blank:]]+MATERIALIZED[[:blank:]]+SOURCE[[:blank:]]+IF[[:blank:]]+NOT[[:blank:]]+EXISTS[[:blank:]]+(?<name>[a-zA-Z0-9_]+)/i,
       :source},
      {~r/^CREATE[[:blank:]]+SINK[[:blank:]]+IF[[:blank:]]+NOT[[:blank:]]+EXISTS[[:blank:]]+(?<name>[a-zA-Z0-9_]+)/i,
       :sink},
      {~r/^CREATE([[:blank:]]+MATERIALIZED)?[[:blank:]]+VIEW([[:blank:]]+IF[[:blank:]]+NOT[[:blank:]]+EXISTS)?[[:blank:]]+(?<name>[a-zA-Z0-9_]+)/i,
       :view}
    ]

  def run_materialize_configs(processors) do
    Procon.Helpers.olog(
      "🎃 PROCON > MATERIALIZE: starting to configure materialize for activated processors (#{inspect(processors)})",
      Procon.Materialize.StarterRun,
      ansi_color: :blue
    )

    Application.get_env(:procon, Processors)
    |> Enum.filter(fn {processor_name, processor_config} ->
      (Enum.empty?(processors) || Enum.any?(processors, &(&1 == processor_name))) &&
        Keyword.get(processor_config, :is_materialize_operator, false)
    end)
    |> Enum.flat_map(fn {processor_name, _processor_config} ->
      materialize_processor_config =
        apply(
          processor_name
          |> Module.concat(MaterializeQueryView),
          :configuration,
          []
        )

      materialize_processor_config.queries
      |> Enum.map(fn query ->
        query_types()
        |> Enum.reduce({Atom.to_string(processor_name), :unknown, nil, query}, fn {regex, type},
                                                                                  {proc_name,
                                                                                   curr_type,
                                                                                   curr_name,
                                                                                   q} ->
          Regex.named_captures(regex, query)
          |> case do
            nil ->
              {proc_name, curr_type, curr_name, q}

            %{"name" => name} ->
              {proc_name, type, name, q}
          end
        end)
      end)
    end)
    |> Enum.reduce(%{views: [], sinks: [], sources: []}, fn query, queries ->
      case query do
        {_proc_name, :view, _name, _q} = res ->
          %{queries | views: [res | queries.views]}

        {_proc_name, :sink, _name, _q} = res ->
          %{queries | sinks: [res | queries.sinks]}

        {_proc_name, :source, _name, _q} = res ->
          %{queries | sources: [res | queries.sources]}
      end
    end)
    |> filter_sources()
    |> run_materialize_registers()
  end

  def filter_sources(%{sinks: sinks, views: views, sources: sources}) do
    views_names = views |> Enum.map(&elem(&1, 2))

    %{
      sinks: sinks,
      views: views,
      sources:
        sources
        |> Enum.reject(fn {proc_name, :source, name, _query} ->
          case Enum.member?(views_names, name) do
            true ->
              Procon.Helpers.olog(
                "Removing source found in views: #{proc_name} -> #{name}",
                Procon.Materialize.Starter,
                ansi_color: :green
              )

              true

            false ->
              false
          end
        end)
        |> Enum.uniq_by(fn {_proc_name, :source, name, _query} -> name end)
    }
  end

  def run_materialize_registers(%{sinks: sinks, views: views, sources: sources}) do
    Procon.Helpers.log(
      "Registering: #{inspect(length(sinks))} sinks, #{inspect(length(views))} views, #{inspect(length(sources))} sources",
      ansi_color: :green
    )

    case :epgsql.connect(get_runtime_config_materialize()) do
      {:ok, epgsql_pid} ->
        sources
        |> run_materialize_registers(epgsql_pid)

        views
        |> run_materialize_registers(epgsql_pid)

        sinks
        |> run_materialize_registers(epgsql_pid)

        :ok = :epgsql.close(epgsql_pid)

        Procon.Helpers.log(
          "👍 done : all processors registered!",
          ansi_color: :blue
        )

      {:error, reason} ->
        Procon.Helpers.olog(
          reason,
          Procon.Materialize.Starter,
          label:
            "🎃❌ PROCON > MATERIALIZE > epgsql.connect error: Unable to configure materialize",
          ansi_color: :blue
        )
    end
  end

  def run_materialize_registers(queries, epgsql_pid) do
    queries
    |> Procon.Parallel.pmap(
      fn query ->
        setup_materialize_for_processor(query, epgsql_pid)
        |> case do
          :ok ->
            nil

          unregistered_query ->
            unregistered_query
        end
      end,
      180_000
    )
    |> Enum.reject(&is_nil/1)
    |> (fn q ->
          Procon.Helpers.olog(
            q
            |> Enum.map(fn {proc_name, type, entity_name, _query} ->
              "#{proc_name} -> #{Atom.to_string(type)} -> #{entity_name}"
            end),
            Procon.Materialize.Starter,
            ansi_color: :blue
          )

          q
        end).()
    |> case do
      [] ->
        Procon.Helpers.log(
          "👍 almost done : all queries registered in this chunk",
          ansi_color: :blue
        )

      unregistered_queries ->
        Procon.Helpers.log("All queries not yet registered.", ansi_color: :blue)

        Procon.Helpers.log(
          unregistered_queries
          |> Enum.map(fn {proc_name, type, entity_name, _query} ->
            "#{proc_name} -> #{Atom.to_string(type)} -> #{entity_name}"
          end),
          ansi_color: :blue
        )

        Procon.Helpers.log("starting another registration with unregistered queries only",
          ansi_color: :blue
        )

        run_materialize_registers(unregistered_queries, epgsql_pid)
    end
  end

  def setup_materialize_for_processor({processor_name, type, entity_name, query}, epgsql_pid) do
    Procon.Helpers.olog(
      "🎃❎ PROCON > MATERIALIZE: Configure materialize for #{processor_name} -> #{Atom.to_string(type)} -> #{entity_name}",
      Procon.Materialize.StarterResult,
      ansi_color: :blue
    )

    result =
      :epgsql.squery(epgsql_pid, query)
      |> case do
        {:ok, [], []} ->
          Procon.Helpers.olog(
            "🎃❎🔧 PROCON > MATERIALIZE > QUERY: executed for #{processor_name} -> #{Atom.to_string(type)} -> #{entity_name}",
            Procon.Materialize.StarterResultOk,
            ansi_color: :blue
          )

          :ok

        {:error, {:error, :error, _reference, :internal_error, error_description, []}} ->
          Procon.Helpers.olog(
            [
              "🎃❌🔧 PROCON > MATERIALIZE > QUERY: unable to execute for #{processor_name} -> #{Atom.to_string(type)} -> #{entity_name}",
              inspect(error_description)
            ],
            Procon.Materialize.StarterResultError,
            ansi_color: :red
          )

          nil

        {:error,
         {:error, :error, _reference, :syntax_error, error_description, [position: pos_str]}} ->
          {pos, _e} = Integer.parse(pos_str)
          margin = 15

          sample =
            query
            |> String.slice(
              Range.new(max(0, pos - margin), min(String.length(query) - 1, pos + margin))
            )

          {line_number, col_number} =
            query
            |> String.slice(Range.new(0, pos))
            |> String.graphemes()
            |> Enum.reduce({1, 1}, fn
              "\n", {line, _col} -> {line + 1, 1}
              _, {line, col} -> {line, col + 1}
            end)

          Procon.Helpers.olog(
            [
              "🎃❌🔧 PROCON > MATERIALIZE > QUERY: unable to execute for #{processor_name} -> #{Atom.to_string(type)} -> #{entity_name}. Syntax error : #{error_description}. At position: #{pos_str}. Line: #{line_number}, column #{col_number}. Near : #{sample}"
            ],
            Procon.Materialize.StarterResultError,
            ansi_color: :red
          )

          nil
      end

    result
    |> case do
      nil ->
        {processor_name, type, entity_name, query}

      _ ->
        :ok
    end
  end
end
