defmodule Procon.Serializers.DynamicTopicBase do
  defmacro __using__(options) do
    quote do
      def message_versions, do: [1]

      def created(event_data, version) do
        case version do
          1 ->
            %{
              entity: event_data.entity,
              inserted_at: event_data.inserted_at,
              partitions_count: event_data.partitions_count,
              processor: event_data.processor,
              tenant_id: event_data.tenant_id,
              topic_name: event_data.topic_name
            }
        end
      end

      def updated(event_data, version) do
        case version do
          1 ->
            %{
              entity: event_data.entity,
              inserted_at: event_data.inserted_at,
              partitions_count: event_data.partitions_count,
              processor: event_data.processor,
              tenant_id: event_data.tenant_id,
              topic_name: event_data.topic_name
            }
        end
      end

      def deleted(event_data, version) do
        case version do
          1 ->
            %{
              entity: event_data.entity,
              inserted_at: event_data.inserted_at,
              partitions_count: event_data.partitions_count,
              processor: event_data.processor,
              tenant_id: event_data.tenant_id,
              topic_name: event_data.topic_name
            }
        end
      end

      def build_partition_key(event_data) do
        event_data.tenant_id
      end

      def topic do
        "procon-dynamic-topics"
      end

      def repo, do: unquote(Keyword.fetch!(options, :repo))

      defoverridable message_versions: 0
      defoverridable created: 2
      defoverridable updated: 2
      defoverridable deleted: 2
      defoverridable build_partition_key: 1
      defoverridable repo: 0
    end
  end
end
