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
            Logger.info(Kernel.inspect(data), options)

          false ->
            nil
        end
    end

    data
  end

  def log(data, options \\ [limit: :infinity]) do
    Logger.info(Kernel.inspect(data), options)
    data
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

  def map_keys_to_atom(map, target_map \\ %{}, materialize_json_attributes \\ [])

  def map_keys_to_atom(map, %_{} = target_struct, materialize_json_attributes) do
    Enum.reduce(
      map,
      target_struct,
      fn {key, value}, atomized_struct ->
        struct!(
          atomized_struct,
          [
            {
              String.to_atom(key),
              case value do
                %{} ->
                  map_keys_to_atom(value, %{}, materialize_json_attributes)

                tmp_value ->
                  case Enum.member?(materialize_json_attributes, String.to_atom(key)) do
                    true -> decode_json_attribute(tmp_value)
                    false -> tmp_value
                  end
              end
            }
          ]
        )
      end
    )
  end

  def map_keys_to_atom(map, target_map, materialize_json_attributes) do
    Enum.reduce(
      map,
      target_map,
      fn {key, value}, atomized_map ->
        Map.put(
          atomized_map,
          String.to_atom(key),
          case value do
            %{} ->
              map_keys_to_atom(value, %{}, materialize_json_attributes)

            tmp_value ->
              case Enum.member?(materialize_json_attributes, String.to_atom(key)) do
                true -> decode_json_attribute(tmp_value)
                false -> tmp_value
              end
          end
        )
      end
    )
  end

  defp decode_json_attribute(value) when is_list(value), do: value |> Enum.map(&(Jason.decode!(&1)))
  defp decode_json_attribute(value), do: Jason.decode!(value)

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

  def remap_materialize_json_attributes(
        message,
        materialize_json_attributes
      ) do
    materialize_json_attributes
    |> Enum.reduce(message, fn materialize_json_attribute, new_message ->
      Kernel.update_in(
        new_message,
        [:after, materialize_json_attribute],
        &(&1 |> get_in([:after, materialize_json_attribute]) |> Jason.decode!())
      )
    end)
  end
end
