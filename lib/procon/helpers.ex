defmodule Procon.Helpers do
  require Logger

  def application_config(value, type) do
    case value do
      {:system, env_var_name} ->
        System.fetch_env!(env_var_name) |> type.()

      value ->
        value
    end
  end

  def olog(data, log_type, options \\ []) do
    Application.fetch_env!(:procon, :logs)
    |> case do
      [] ->
        nil

      log_types ->
        Enum.member?(log_types, log_type)
        |> case do
          true ->
            Logger.info(data, options)

          false ->
            nil
        end
    end
  end

  def log(data, options \\ []) do
    Logger.info(data, options)
  end

  def inspect(data, label \\ "", color \\ :red) do
    IO.inspect(
      data,
      label: label,
      limit: :infinity,
      syntax_colors: [
        atom: color,
        binary: color,
        boolean: color,
        list: color,
        map: color,
        nil: color,
        number: color,
        pid: color,
        regex: color,
        string: color,
        tuple: color
      ]
    )
  end

  def map_keys_to_atom(map, struct) do
    Enum.reduce(
      map,
      struct,
      fn {key, value}, atomized_map ->
        Map.put(
          atomized_map,
          String.to_atom(key),
          case value do
            %{} -> map_keys_to_atom(value, %{})
            _ -> value
          end
        )
      end
    )
  end

  def fetch_last_message(brod_client, topic, partition) do
    :brod.fetch(
      brod_client,
      topic,
      partition,
      (:brod.resolve_offset(brod_client, topic, partition)
       |> elem(1)) - 1
    )
    |> elem(1)
    |> elem(1)
    |> List.first()
    |> elem(3)
    |> Avrora.decode()
    |> elem(1)
  end

  def is_debezium_create(message) do
    Map.get(message, "before") == nil && is_map(Map.get(message, "after"))
  end
end
