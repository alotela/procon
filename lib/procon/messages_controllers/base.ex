defmodule Procon.MessagesControllers.Base do
  defmacro __using__(_options) do
    quote do
      import Procon.MessagesControllers.Base.Helpers

      def create(event, options) do
        do_create(__MODULE__, event, options)
      end

      def delete(event, options) do
        do_delete(__MODULE__, event, options)
      end

      def update(event, options) do
        do_update(__MODULE__, event, options)
      end

      def before_create(event_data, options), do: event_data
      def process_create(event, options), do: process_create(__MODULE__, event, options)
      def after_create(event_data, options), do: {:ok, event_data}
      def after_create_transaction(event_data, options), do: {:ok, event_data}
      def before_update(event_data, options), do: event_data
      def process_update(event, options), do: process_update(__MODULE__, event, options)
      def after_update(event_data, options), do: {:ok, event_data}
      def after_update_transaction(event_data, options), do: {:ok, event_data}
      def before_delete(event_data, options), do: event_data
      def process_delete(event, options), do: process_delete(__MODULE__, event, options)
      def after_delete(event_data, options), do: {:ok, event_data}
      def after_delete_transaction(event_data, options), do: {:ok, event_data}

      defoverridable after_create: 2,
                     before_create: 2,
                     before_delete: 2,
                     after_create_transaction: 2,
                     after_delete: 2,
                     after_delete_transaction: 2,
                     after_update: 2,
                     after_update_transaction: 2,
                     before_update: 2,
                     create: 2,
                     delete: 2,
                     update: 2,
                     process_create: 2,
                     process_delete: 2,
                     process_update: 2
    end
  end

  defmodule Helpers do
    require Logger
    require Record

    def do_create(controller, event, options) do
      options.datastore.transaction(fn ->
        controller.process_create(event, options)
        |> case do
          {:ok, event_data} ->
            {:ok, final_event_data} = controller.after_create(event_data, options)

            {:ok, final_event_data} =
              forward_entity(
                final_event_data,
                if(final_event_data.record_from_db, do: :updated, else: :created),
                Map.get(options, :serializer, nil),
                Map.get(options, :serializer_validation, nil)
              )

            {:ok, final_event_data} =
              enqueue_realtime(
                final_event_data,
                if(final_event_data.record_from_db, do: :updated, else: :created),
                Map.get(options, :send_realtime, nil)
              )

            :ok =
              Procon.PartitionOffsetHelpers.update_partition_offset(
                options.topic,
                options.partition,
                options.offset,
                options.datastore,
                options.processor_name
              )

            final_event_data

          {:error, ecto_changeset} ->
            Procon.Helpers.inspect(
              ecto_changeset,
              "#{options.processing_id}@@PROCON FLOW ERROR : ecto changeset : unable to save entity : #{
                options.processor_name
              }/#{options.topic}/#{options.partition}/#{options.offset}"
            )

            options.datastore.rollback(ecto_changeset)
        end
      end)
      |> case_final_event_data(&controller.after_create_transaction/2, options)
    end

    def case_final_event_data(transaction_result, after_transaction, options) do
      transaction_result
      |> case do
        {:ok, final_event_data} ->
          Procon.PartitionOffsetHelpers.update_partition_offset_ets(
            options.processor_name,
            options.topic,
            options.partition,
            options.offset,
            options.processing_id
          )
          |> case do
            true ->
              start_forward_production(Map.get(options, :serializer, nil))

              start_realtime_production(
                Map.get(final_event_data, :entity_realtime_event_serializer, nil)
              )

              after_transaction.(final_event_data, options)

              :ok

            _ ->
              {:error, :unable_to_update_offset_in_ets}
          end

        {:error, error} ->
          {:error, error}
      end
    end

    def process_create(controller, event, options) do
      event_data =
        record_and_body_from_event(event, true, options)
        |> event_data_with_attributes(options.keys_mapping)
        |> controller.before_create(options)

      options.model.messages_create_changeset(event_data.record, event_data.attributes)
      |> options.datastore.insert_or_update()
      |> case do
        {:ok, struct} -> {:ok, Map.put(event_data, :record, struct)}
        {:error, ecto_changeset} -> {:error, ecto_changeset}
      end
    end

    def forward_entity(event_data, _type, nil, nil),
      do: {:ok, Map.put(event_data, :entity_forwarded, false)}

    def forward_entity(event_data, type, serializer, serializer_validation) do
      case serializer_validation do
        nil ->
          true

        _ ->
          serializer_validation.(event_data.record, type)
      end
      |> case do
        true ->
          Procon.MessagesEnqueuers.Ecto.enqueue_event(
            event_data.record,
            serializer,
            type
          )

          {:ok, Map.put(event_data, :entity_forwarded, true)}

        false ->
          {:ok, Map.put(event_data, :entity_forwarded, false)}
      end
    end

    def enqueue_realtime(event_data, _type, nil),
      do:
        {:ok,
         event_data
         |> Map.put(:entity_realtime_event_enqueued, false)
         |> Map.put(:entity_realtime_event_serializer, nil)}

    def enqueue_realtime(event_data, type, realtime_event) do
      realtime_event.(type, event_data.record)
      |> case do
        nil ->
          {:ok,
           event_data
           |> Map.put(:entity_realtime_event_enqueued, false)
           |> Map.put(:entity_realtime_event_serializer, nil)}

        %{event: event, channel: channel, serializer: serializer} ->
          Procon.MessagesEnqueuers.Ecto.enqueue_rtevent(
            %{channel: channel, event: event},
            serializer
          )

          {
            :ok,
            event_data
            |> Map.put(:entity_realtime_event_enqueued, true)
            |> Map.put(:entity_realtime_event_serializer, serializer)
          }
      end
    end

    def do_update(controller, event, options) do
      options.datastore.transaction(fn ->
        controller.process_update(event, options)
        |> case do
          {:ok, event_data} ->
            {:ok, final_event_data} = controller.after_update(event_data, options)

            {:ok, final_event_data} =
              forward_entity(
                final_event_data,
                :updated,
                Map.get(options, :serializer, nil),
                Map.get(options, :serializer_validation, nil)
              )

            {:ok, final_event_data} =
              enqueue_realtime(
                final_event_data,
                :updated,
                Map.get(options, :send_realtime, nil)
              )

            :ok =
              Procon.PartitionOffsetHelpers.update_partition_offset(
                options.topic,
                options.partition,
                options.offset,
                options.datastore,
                options.processor_name
              )

            final_event_data

          {:error, ecto_changeset} ->
            Procon.Helpers.inspect(
              ecto_changeset,
              "#{options.processing_id}@@PROCON FLOW ERROR : ecto changeset : unable to save entity : #{
                options.processor_name
              }/#{options.topic}/#{options.partition}/#{options.offset}"
            )

            options.datastore.rollback(ecto_changeset)
        end
      end)
      |> case_final_event_data(&controller.after_update_transaction/2, options)
    end

    def start_forward_production(nil), do: nil

    def start_forward_production(serializer) do
      Procon.MessagesProducers.ProducersStarter.start_topic_production(serializer)
    end

    def start_realtime_production(nil), do: nil

    def start_realtime_production(serializer) do
      Procon.MessagesProducers.ProducersStarter.start_topic_production(serializer)
    end

    def process_update(controller, event, options) do
      event_data =
        record_and_body_from_event(event, true, options)
        |> event_data_with_attributes(options.keys_mapping)
        |> controller.before_update(options)

      options.model.messages_update_changeset(event_data.record, event_data.attributes)
      |> options.datastore.insert_or_update()
      |> case do
        {:ok, struct} -> {:ok, Map.put(event_data, :record, struct)}
        {:error, ecto_changeset} -> {:error, ecto_changeset}
      end
    end

    def event_data_with_attributes(event_data, atom_changeset) do
      attributes =
        for {key, val} <- event_data.body, into: %{} do
          {Map.get(atom_changeset, key, String.to_atom(key)), val}
        end

      Map.put(event_data, :attributes, attributes)
    end

    def do_delete(controller, event, options) do
      options.datastore.transaction(fn ->
        controller.process_delete(event, options)
        |> case do
          {:ok, event_data} ->
            {:ok, final_event_data} = controller.after_delete(event_data, options)

            {:ok, final_event_data} =
              forward_entity(
                final_event_data,
                :deleted,
                Map.get(options, :serializer, nil),
                Map.get(options, :serializer_validation, nil)
              )

            {:ok, final_event_data} =
              enqueue_realtime(
                final_event_data,
                :deleted,
                Map.get(options, :send_realtime, nil)
              )

            :ok =
              Procon.PartitionOffsetHelpers.update_partition_offset(
                options.topic,
                options.partition,
                options.offset,
                options.datastore,
                options.processor_name
              )

            final_event_data

          {:error, ecto_changeset} ->
            Procon.Helpers.inspect(
              ecto_changeset,
              "#{options.processing_id}@@PROCON FLOW ERROR : ecto changeset : unable to save entity : #{
                options.processor_name
              }/#{options.topic}/#{options.partition}/#{options.offset}"
            )

            options.datastore.rollback(ecto_changeset)
        end
      end)
      |> case_final_event_data(&controller.after_delete_transaction/2, options)
    end

    def process_delete(controller, event, options) do
      event_data =
        record_and_body_from_event(event, false, options)
        |> controller.before_delete(options)

      case event_data.record do
        nil ->
          Logger.warn("No match for deleted #{inspect(options.model)} in event #{inspect(event)}")

          {:ok, event_data}

        _ ->
          Logger.info("Deleting #{inspect(options.model)} with id #{event_data.record.id}")

          case options.datastore.delete(event_data.record) do
            {:ok, struct} -> {:ok, Map.put(event_data, :record, struct)}
            {:error, ecto_changeset} -> {:error, ecto_changeset}
          end
      end
    end

    def record_and_body_from_event(event, return_record, options) do
      body = extract_versioned_body(event, options)
      master_keys = normalize_master_key(options.master_key)

      if length(master_keys) > 0 do
        query =
          master_keys
          |> Enum.map(fn {repo_key, body_key} ->
            body
            |> Map.get(body_key, nil)
            |> case do
              nil ->
                IO.inspect(event,
                  label:
                    "⚠️PROCON ALERT : #{options.processor_name} : the master_key value in event body is nil for key #{
                      repo_key
                    } configured",
                  syntax_colors: [
                    atom: :red,
                    binary: :red,
                    boolean: :red,
                    list: :red,
                    map: :red,
                    number: :red,
                    regex: :red,
                    string: :red,
                    tuple: :red
                  ]
                )

                nil

              value ->
                {repo_key, value}
            end
          end)

        query
        |> Enum.any?(&is_nil/1)
        |> case do
          true ->
            nil

          false ->
            options.datastore.get_by(options.model, query)
        end
      else
        case Map.get(body, "id") do
          nil ->
            IO.inspect(event,
              label:
                "⚠️PROCON ALERT : #{options.processor_name} : \"id\" in body is nil to find record in database. Maybe you need to specify master_key in processor config for this entity ?",
              syntax_colors: [
                atom: :red,
                binary: :red,
                boolean: :red,
                list: :red,
                map: :red,
                number: :red,
                regex: :red,
                string: :red,
                tuple: :red
              ]
            )

          id ->
            options.datastore.get(options.model, id)
        end
      end
      |> case do
        nil ->
          %{
            body: body,
            record_from_db: false,
            record: if(return_record, do: struct(options.model), else: nil)
          }

        struct ->
          %{body: body, record_from_db: true, record: struct}
      end
    end

    def normalize_master_key({_repo_key, _body_key} = master_key), do: [master_key]

    def normalize_master_key(master_keys) when is_list(master_keys),
      do: master_keys

    def normalize_master_key(nil), do: []
    def normalize_master_key(master_key) when is_map(master_key), do: Map.to_list(master_key)

    @spec extract_versioned_body(map, map) :: map
    def extract_versioned_body(event, options) do
      get_in(event, ["body", Map.get(options, :event_version) |> to_string()])
    end
  end
end
