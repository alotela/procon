defmodule Procon.Serializers.RealtimeBase do
  defmacro __using__(options) do
    quote do
      def message_versions, do: [1]

      def created(event_data, version) do
        case version do
          1 ->
            %{
              channel: Map.get(event_data, :channel, nil),
              session_id: Map.get(event_data, :session_id, nil),
              event: event_data.event,
              custom_data: Map.get(event_data, :custom_data, nil)
            }
        end
      end

      def build_partition_key(event_data), do: event_data.event

      def topic(), do: "procon-realtime"

      def repo(), do: unquote(Keyword.fetch!(options, :repo))

      def threshold(), do: unquote(Keyword.fetch!(options, :threshold))

      defoverridable message_versions: 0
      defoverridable created: 2
      defoverridable build_partition_key: 1
      defoverridable repo: 0
      defoverridable threshold: 0
    end
  end
end
