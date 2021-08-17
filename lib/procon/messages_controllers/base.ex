defmodule Procon.MessagesControllers.Base do
  defmacro __using__(_options) do
    quote do
      import Procon.MessagesControllers.Base.Helpers
      import Procon.MessagesControllers.EventDataAccessors

      @spec create(
              Procon.Types.DebeziumMessage.t(),
              Procon.Types.BaseMethodOptions.t()
            ) :: any
      def create(
            %Procon.Types.DebeziumMessage{} = message,
            %Procon.Types.BaseMethodOptions{} = options
          ) do
        do_create(__MODULE__, message, options)
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

    @pg_epoch DateTime.from_iso8601("2000-01-01T00:00:00Z")

    def do_create(controller, event, options) do
      options.datastore.transaction(fn ->
        controller.process_create(event, options)
        |> case do
          {:ok, event_data} ->
            {:ok, final_event_data} = controller.after_create(event_data, options)

            {:ok, final_event_data} =
              enqueue_realtime(
                final_event_data,
                if(final_event_data.record_from_db, do: :updated, else: :created),
                options
              )

            {:ok, final_event_data} =
              message_processed_hook(
                final_event_data,
                if(final_event_data.record_from_db, do: :updated, else: :created),
                options
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
              "#{options.processing_id}@@PROCON FLOW ERROR : ecto changeset : unable to save entity : #{options.processor_name}/#{options.topic}/#{options.partition}/#{options.offset}"
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
              # start_forward_production(Map.get(options, :serializer, nil))

              # start_realtime_production(
              # Map.get(final_event_data, :entity_realtime_event_serializer, nil)
              # )

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
        record_from_datastore(event, true, options)
        |> add_new_attributes(options)
        |> controller.before_create(options)

      options.model.messages_create_changeset(
        event_data.record,
        Map.get(event_data, :new_attributes, event_data.event.after)
      )
      |> options.datastore.insert_or_update(returning: true)
      |> case do
        {:ok, struct} -> {:ok, Map.put(event_data, :recorded_struct, struct)}
        {:error, ecto_changeset} -> {:error, ecto_changeset}
      end
    end

    def enqueue_realtime(event_data, _type, _options = %{realtime_builder: nil}),
      do:
        {:ok,
         event_data
         |> Map.put(:entity_realtime_event_enqueued, false)}

    @spec enqueue_realtime(map, any, any) ::
            {:ok, %{:entity_realtime_event_enqueued => boolean, optional(any) => any}}
    def enqueue_realtime(event_data, type, options = %{realtime_builder: realtime_builder}) do
      realtime_builder.(type, event_data)
      |> case do
        nil ->
          {:ok, Map.put(event_data, :entity_realtime_event_enqueued, false)}

        %Procon.Schemas.ProconRealtime{} = realtime ->
          Procon.MessagesProducers.Realtime.send_rtevent(
            Map.put(
              realtime,
              :metadata,
              Map.merge(
                case type do
                  :deleted ->
                    Map.get(event_data.event.after, :metadata, %{})

                  _ ->
                    Map.get(event_data.recorded_struct, :metadata, %{})
                end ||
                  %{},
                Map.get(realtime, :metadata, %{}) || %{}
              )
            ),
            options.datastore,
            Map.get(options, :rt_threshold, 1000)
          )

          {:ok, Map.put(event_data, :entity_realtime_event_enqueued, true)}

        [] ->
          Logger.info(
            "👹🤡 Procon > Base > enqueue_realtime : empty list returned > type : #{type}, event_data: #{inspect(event_data)}"
          )

          {:ok, Map.put(event_data, :entity_realtime_event_enqueued, false)}

        [_ | _] = realtime_events ->
          realtime_events
          |> Enum.map(fn
            %Procon.Schemas.ProconRealtime{} = realtime ->
              Procon.MessagesProducers.Realtime.send_rtevent(
                Map.put(
                  realtime,
                  :metadata,
                  Map.merge(
                    Map.get(event_data.recorded_struct, :metadata, %{}) || %{},
                    Map.get(realtime, :metadata, %{}) || %{}
                  )
                ),
                options.datastore,
                Map.get(options, :rt_threshold, 1000)
              )

            _ ->
              nil
          end)

          {:ok, Map.put(event_data, :entity_realtime_event_enqueued, true)}
      end
    end

    def message_processed_hook(event_data, _type, _options = %{message_processed_hook: nil}),
      do: {:ok, event_data}

    def message_processed_hook(
          event_data,
          type,
          %{message_processed_hook: message_processed_hook}
        ),
        do: message_processed_hook.(type, event_data)

    def do_update(controller, event, options) do
      options.datastore.transaction(fn ->
        controller.process_update(event, options)
        |> case do
          {:ok, event_data} ->
            {:ok, final_event_data} = controller.after_update(event_data, options)

            {:ok, final_event_data} =
              enqueue_realtime(
                final_event_data,
                :updated,
                options
              )

            {:ok, final_event_data} =
              message_processed_hook(
                final_event_data,
                :updated,
                options
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
              "#{options.processing_id}@@PROCON FLOW ERROR : ecto changeset : unable to save entity : #{options.processor_name}/#{options.topic}/#{options.partition}/#{options.offset}"
            )

            options.datastore.rollback(ecto_changeset)
        end
      end)
      |> case_final_event_data(&controller.after_update_transaction/2, options)
    end

    def process_update(controller, event, options) do
      event_data =
        record_from_datastore(event, true, options)
        |> add_new_attributes(options)
        |> controller.before_update(options)

      options.model.messages_update_changeset(
        event_data.record,
        Map.get(event_data, :new_attributes, event_data.event.after)
      )
      |> options.datastore.insert_or_update()
      |> case do
        {:ok, struct} -> {:ok, Map.put(event_data, :recorded_struct, struct)}
        {:error, ecto_changeset} -> {:error, ecto_changeset}
      end
    end

    def add_new_attributes(
          %{event: %{after: _, before: nil}} = event_data,
          %{
            keys_mapping: %{created: created_mapping}
          } = options
        ) do
      add_new_attributes(event_data, %{
        drop_event_attributes: options.drop_event_attributes,
        keys_mapping: created_mapping
      })
    end

    def add_new_attributes(
          %{event: %{after: _, before: _}} = event_data,
          %{
            keys_mapping: %{updated: updated_mapping}
          } = options
        ) do
      add_new_attributes(event_data, %{
        drop_event_attributes: options.drop_event_attributes,
        keys_mapping: updated_mapping
      })
    end

    def add_new_attributes(event_data, %{keys_mapping: keys_mapping})
        when keys_mapping == %{} do
      event_data
    end

    def add_new_attributes(event_data, %{keys_mapping: func} = options) when is_function(func) do
      func.(event_data, options)
    end

    def add_new_attributes(event_data, options) do
      attributes =
        Enum.reduce(options.keys_mapping, %{}, fn {key_in_event, key_in_new_attributes},
                                                  reduced_attributes ->
          case key_in_new_attributes do
            nil ->
              reduced_attributes

            new_key ->
              case key_in_event do
                :event_timestamp ->
                  event_data.event.timestamp

                :event_timestamp_datetime ->
                  event_data.event.timestamp |> ulid_to_datetime()

                :create_event_timestamp_datetime ->
                  case Map.get(event_data.event, :before, nil) do
                    nil ->
                      event_data.event.timestamp |> ulid_to_datetime()

                    _ ->
                      :procon_bypass
                  end

                :topic ->
                  options.topic

                [_ | _] ->
                  get_in(event_data.event.after, key_in_event)

                _ ->
                  Map.get(event_data.event.after, key_in_event)
              end
              |> case do
                :procon_bypass ->
                  reduced_attributes

                value ->
                  Map.put(
                    reduced_attributes,
                    key_in_new_attributes,
                    case new_key do
                      f when is_function(f) ->
                        f.(value)

                      _ ->
                        value
                    end
                  )
              end
          end
        end)

      new_attributes = Map.merge(event_data.event.after, attributes)

      new_attributes =
        case Map.has_key?(options, :drop_event_attributes) &&
               Kernel.map_size(options.drop_event_attributes) > 0 do
          true ->
            Map.drop(
              new_attributes,
              [
                Map.get(options.drop_event_attributes, :always, [])
                | Map.get(
                    options.drop_event_attributes,
                    case event_data.event.before do
                      nil -> :on_create
                      _ -> :on_update
                    end,
                    []
                  )
              ]
            )

          false ->
            new_attributes
        end

      Map.put(event_data, :new_attributes, new_attributes)
    end

    def pgtimestamp_to_timestamp(microsecond_offset) do
      {:ok, epoch, 0} = @pg_epoch

      DateTime.add(epoch, microsecond_offset, :millisecond)
    end

    def ulid_to_datetime(ulid_timestamp) do
      <<timestamp_in_ms::unsigned-size(48), _rest::binary>> =
        ulid_timestamp
        |> Ecto.ULID.dump()
        |> elem(1)

      timestamp_in_ms
      |> pgtimestamp_to_timestamp()
    end

    def do_delete(controller, event, options) do
      options.datastore.transaction(fn ->
        controller.process_delete(event, options)
        |> case do
          {:ok, event_data} ->
            {:ok, final_event_data} = controller.after_delete(event_data, options)

            {:ok, final_event_data} =
              case Map.get(event_data, :record_deleted, true) do
                true ->
                  {:ok, final_event_data} =
                    enqueue_realtime(
                      final_event_data,
                      :deleted,
                      options
                    )

                  message_processed_hook(
                    final_event_data,
                    :deleted,
                    options
                  )

                false ->
                  {:ok, final_event_data}
              end

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
              "#{options.processing_id}@@PROCON FLOW ERROR : ecto changeset : unable to save entity : #{options.processor_name}/#{options.topic}/#{options.partition}/#{options.offset}"
            )

            options.datastore.rollback(ecto_changeset)
        end
      end)
      |> case_final_event_data(&controller.after_delete_transaction/2, options)
    end

    def process_delete(controller, event, options) do
      event_data =
        record_from_datastore(event, false, options)
        |> controller.before_delete(options)

      case event_data.record do
        nil ->
          Logger.warn("No match for deleted #{inspect(options.model)} in event #{inspect(event)}")

          {:ok, Map.put(event_data, :record_deleted, false)}

        _ ->
          Logger.info("Deleting #{inspect(options.model)} with id #{event_data.record.id}")

          case options.datastore.delete(event_data.record) do
            {:ok, struct} -> {:ok, Map.put(event_data, :recorded_struct, struct)}
            {:error, ecto_changeset} -> {:error, ecto_changeset}
          end
      end
    end

    @spec record_from_datastore(
            Procon.Types.DebeziumMessage.event(),
            boolean,
            Procon.Types.BaseMethodOptions.t()
          ) :: %{
            event: Procon.Types.DebeziumMessage.event(),
            record: Procon.Types.procon_entity() | nil,
            record_from_db: boolean
          }
    def record_from_datastore(event, return_record, options) do
      master_keys = normalize_master_key(options.master_key)

      get_new_or_old = fn
        :get, %{before: nil, after: new}, next ->
          next.(new)

        :get, %{before: before}, next ->
          next.(before)
      end

      if length(master_keys) > 0 do
        query =
          master_keys
          |> Enum.map(fn {repo_key, body_key} ->
            event
            |> get_in([get_new_or_old, body_key])
            |> case do
              nil ->
                Procon.Helpers.log([
                  "⚠️PROCON ALERT : #{options.processor_name} : the master_key value in event body is nil for key #{repo_key}. Typo error ?",
                  event
                ])

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
        case get_in(event, [get_new_or_old, :id]) do
          nil ->
            Procon.Helpers.log([
              "⚠️PROCON ALERT : #{options.processor_name} : \"id\" in body is nil to find record in database. Maybe you need to specify master_key in processor config for this entity ?",
              event
            ])

          id ->
            options.datastore.get(options.model, id)
        end
      end
      |> case do
        nil ->
          %{
            event: event,
            record: if(return_record, do: struct(options.model), else: nil),
            record_from_db: false
          }

        struct ->
          %{
            event: event,
            record: struct,
            record_from_db: true
          }
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
