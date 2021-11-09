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
              Kernel.update_in(
                new_payload,
                [:after, materialize_json_attribute],
                &(&1 |> Jason.encode!())
              )
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
end
