defmodule Procon.MessagesControllers.EventDataAccessors do
  def read_new_attributes(event_data), do: event_data.new_attributes

  def write_new_attributes(event_data, new_attributes_map) do
    Map.put(event_data, :new_attributes, new_attributes_map)
  end

  def read_root_event_new_payload(%Procon.Types.DebeziumMessage{} = event), do: event.after

  def read_before_payload(event_data), do: event_data.event.before

  def read_before_payload_value(event_data, path),
    do: get_in(event_data, [:event, Access.key(:before) | path])

  def read_payload(event_data), do: event_data.event.after

  def read_payload_value(event_data, path),
    do: get_in(event_data, [:event, Access.key(:after) | path])

  def write_record(event_data, record) do
    Map.put(event_data, :record, record)
  end

  def read_recorded_struct(event_data), do: get_in(event_data, [:recorded_struct])

  def read_metadata(event_data, key) when is_binary(key)  do
    Procon.Helpers.log("⚠️ To read a metadata, the key must be an atom. String received as key! (automatic convesion done, but this it to remove this warning)")
    read_recorded_struct(event_data) |> Map.get(:metadata, %{}) |> Map.get(String.to_atom(key))
    |> case do
      nil ->
        Procon.Helpers.log("⚠️ You search a metadata with key #{key} (as string), but it was not found. nil returned, no realtime message might be sent!")
        nil
      data ->
        data
      end
  end

  def read_metadata(event_data, key) when is_atom(key)  do
    read_recorded_struct(event_data) |> Map.get(:metadata, %{}) |> Map.get(key)
    |> case do
      nil ->
        Procon.Helpers.log("⚠️ You search a metadata with key #{key}, but it was not found. nil returned, no realtime message might be sent!")
        nil
      data ->
        data
      end
    end

  def read_recorded_struct_value(event_data, key),
    do: read_recorded_struct(event_data) |> Map.get(key)
end
