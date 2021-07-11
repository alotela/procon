defmodule Procon.MessagesControllers.EventDataAccessors do
  def read_new_attributes(event_data), do: event_data.new_attributes

  def write_new_attributes(event_data, new_attributes_map) do
    Map.put(event_data, :new_attributes, new_attributes_map)
  end

  def read_debezium_after(%Procon.Types.DebeziumMessage{} = event), do: event.after

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

  def read_metadata(event_data, key),
    do: read_recorded_struct(event_data) |> Map.get(:metadata, %{}) |> Map.get(key)

  def read_recorded_struct_value(event_data, key),
    do: read_recorded_struct(event_data) |> Map.get(key)
end
