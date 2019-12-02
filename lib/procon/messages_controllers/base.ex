defmodule Procon.MessagesControllers.Base do
  defmacro __using__(_options) do
    quote do
      import Procon.MessagesControllers.Base.Helpers

      def create(event, options) do
        do_create(__MODULE__, event, options)
      end

      def destroy(event, options) do
        do_destroy(__MODULE__, event, options)
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
      def before_destroy(event_data, options), do: event_data
      def process_destroy(event, options), do: process_destroy(__MODULE__, event, options)
      def after_destroy(event_data, options), do: {:ok, event_data}

      defoverridable after_create: 2,
                     before_create: 2,
                     before_destroy: 2,
                     after_create_transaction: 2,
                     after_destroy: 2,
                     after_update: 2,
                     after_update_transaction: 2,
                     before_update: 2,
                     create: 2,
                     destroy: 2,
                     update: 2,
                     process_create: 2,
                     process_destroy: 2,
                     process_update: 2
    end
  end

  defmodule Helpers do
    import Ecto.Query
    require Logger
    require Record
    alias Procon.Schemas.Ecto.ProconConsumerIndex

    def do_create(controller, event, options) do
      if message_not_already_processed?(event, options) do
        {:ok, {final_event_data, consumer_message_index}} =
          options.datastore.transaction(fn ->
            controller.process_create(event, options)
            |> case do
              {:ok, event_data} ->
                {:ok, final_event_data} = controller.after_create(event_data, options)
                consumer_message_index = update_consumer_message_index(event, options)
                {final_event_data, consumer_message_index}

              {:error, ecto_changeset} ->
                Logger.warn(
                  "Unable to create #{inspect(options.model)} with event #{inspect(event)}@@ changeset : #{
                    inspect(ecto_changeset)
                  }"
                )

                options.datastore.rollback(ecto_changeset)
            end
          end)

        update_consumer_message_index_ets(consumer_message_index, options.processor_name)
        controller.after_create_transaction(final_event_data, options)
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

    def do_update(controller, event, options) do
      if message_not_already_processed?(event, options) do
        {:ok, {final_event_data, consumer_message_index}} =
          options.datastore.transaction(fn ->
            controller.process_update(event, options)
            |> case do
              {:ok, event_data} ->
                {:ok, final_event_data} = controller.after_update(event_data, options)
                consumer_message_index = update_consumer_message_index(event, options)
                {final_event_data, consumer_message_index}

              {:error, ecto_changeset} ->
                Logger.warn(
                  "Unable to update #{inspect(options.model)} with event #{inspect(event)}@@ changeset : #{
                    inspect(ecto_changeset)
                  }"
                )

                options.datastore.rollback(ecto_changeset)
            end
          end)

        update_consumer_message_index_ets(consumer_message_index, options.processor_name)
        controller.after_update_transaction(final_event_data, options)
      end
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

    def do_destroy(controller, event, options) do
      if message_not_already_processed?(event, options) do
        {:ok, consumer_message_index} =
          options.datastore.transaction(fn ->
            controller.process_destroy(event, options)
            |> case do
              {:ok, event_data} ->
                {:ok, _} = controller.after_destroy(event_data, options)
                update_consumer_message_index(event, options)

              {:error, ecto_changeset} ->
                Logger.info(
                  "Unable to destroy #{inspect(options.model)} in event #{inspect(event)}@@#{
                    inspect(ecto_changeset)
                  }"
                )

                options.datastore.rollback(ecto_changeset)
            end
          end)

        update_consumer_message_index_ets(consumer_message_index, options.processor_name)
      end
    end

    def process_destroy(controller, event, options) do
      event_data =
        record_and_body_from_event(event, false, options)
        |> controller.before_destroy(options)

      case event_data.record do
        nil ->
          Logger.warn(
            "No match for destroyed #{inspect(options.model)} in event #{inspect(event)}"
          )

          {:ok, event_data}

        _ ->
          Logger.info("Destroying #{inspect(options.model)} with id #{event_data.record.id}")

          case options.datastore.delete(event_data.record) do
            {:ok, struct} -> {:ok, Map.put(event_data, :record, struct)}
            {:error, ecto_changeset} -> {:error, ecto_changeset}
          end
      end
    end

    def message_not_already_processed?(event, options) do
      Map.get(event, "index") > find_last_processed_message_id(event, options)
    end

    def update_consumer_message_index_ets(consumer_message_index, processor_name) do
      true =
        :ets.insert(
          processor_name,
          {build_ets_key(consumer_message_index), consumer_message_index}
        )
    end

    defp get_consumer_message_index_ets(ets_key, ets_table) do
      [{^ets_key, struct}] = :ets.lookup(ets_table, ets_key)
      struct
    end

    def update_consumer_message_index(event, options) do
      topic = Map.get(options, :topic)
      partition = Map.get(event, :partition)
      ets_key = build_ets_key(topic, partition)
      message_id = Map.get(event, "index")
      struct = get_consumer_message_index_ets(ets_key, options.processor_name)

      case Procon.Schemas.Ecto.ProconConsumerIndex.changeset(struct, %{message_id: message_id})
           |> options.datastore.update() do
        {:ok, updated_struct} ->
          updated_struct

        {:error, ecto_changeset} ->
          Logger.info(
            "Unable to update message index #{inspect(ecto_changeset)} with id #{struct.id}"
          )

          options.datastore.rollback(ecto_changeset)
      end
    end

    def build_ets_key(topic, partition), do: "#{topic}_#{partition}"

    def build_ets_key(consumer_message_index) do
      "#{consumer_message_index.topic}_#{consumer_message_index.partition}"
    end

    defp find_last_processed_message_id(event, options) do
      topic = Map.get(options, :topic)
      partition = Map.get(event, :partition)
      ets_key = build_ets_key(topic, partition)

      last_processed_message_id =
        case :ets.lookup(options.processor_name, ets_key) do
          [] ->
            case from(c in Procon.Schemas.Ecto.ProconConsumerIndex,
                   where: [topic: ^topic, partition: ^partition]
                 )
                 |> options.datastore.one do
              nil ->
                struct =
                  options.datastore.insert!(%ProconConsumerIndex{
                    message_id: -1,
                    partition: partition,
                    topic: topic
                  })

                update_consumer_message_index_ets(struct, options.processor_name)
                struct.message_id

              struct ->
                update_consumer_message_index_ets(struct, options.processor_name)
                struct.message_id
            end

          [{^ets_key, struct}] ->
            struct.message_id
        end

      last_processed_message_id
    end

    def record_and_body_from_event(event, return_record, options) do
      body = get_in(event, ["body", Map.get(options, :event_version) |> to_string()])

      if !is_nil(options.master_key) do
        options.datastore.get_by(
          options.model,
          [{elem(options.master_key, 0), body[elem(options.master_key, 1)]}]
        )
      else
        options.datastore.get(options.model, body["id"])
      end
      |> case do
        nil ->
          %{
            body: body,
            record: if(return_record, do: struct(options.model), else: nil)
          }

        struct ->
          %{body: body, record: struct}
      end
    end
  end
end
