defmodule Procon.MessagesProducers.Kafka do
  def build_message(pa_key, pa_payload, pa_schema_name, pa_serialization, state) do
    [serialized_key, serialized_payload] =
      case pa_serialization do
        :json ->
          [pa_key, Jason.encode!(pa_payload)]

        :avro ->
          [
            %{"id" => pa_key}
            |> Avrora.encode(
              schema_name: "procon-generic-key",
              format: :registry
            )
            |> elem(1),
            Map.get(state, :materialize_json_attributes, [])
            |> Enum.reduce(pa_payload, fn materialize_json_attribute, new_payload ->
              new_payload
              |> case do
                %{after: after_, before: before_}
                when not is_nil(after_) and not is_nil(before_) ->
                  new_payload
                  |> Kernel.update_in(
                    [:after, materialize_json_attribute],
                    &json_encode_attribute/1
                  )
                  |> Kernel.update_in(
                    [:before, materialize_json_attribute],
                    &json_encode_attribute/1
                  )

                %{after: after_} when not is_nil(after_) ->
                  new_payload
                  |> Kernel.update_in(
                    [:after, materialize_json_attribute],
                    &json_encode_attribute/1
                  )

                %{before: before_} when not is_nil(before_) ->
                  new_payload
                  |> Kernel.update_in(
                    [:before, materialize_json_attribute],
                    &json_encode_attribute/1
                  )
              end
            end)
            |> Avrora.encode(
              schema_name: pa_schema_name,
              format: :registry
            )
            |> elem(1)
          ]
      end

    {serialized_key, serialized_payload}
  end

  defp json_encode_attribute(attr_value) when is_list(attr_value), do: attr_value |> Enum.map(&(&1 |> Jason.encode!()))
  defp json_encode_attribute(attr_value), do: Jason.encode!(attr_value)
end
