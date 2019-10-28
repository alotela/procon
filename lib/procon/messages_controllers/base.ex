defmodule Procon.MessageControllers.Base do
  defmacro __using__(_options) do
    quote do
      import Procon.MessageControllers.Base.Helpers

      def datastore, do: @datastore
      def keys_mapping, do: @keys_mapping
      def master_key, do: @master_key
      def message_version, do: @message_version
      def model, do: @model
      def topic, do: @topic

      def create(event) do
        try do
          do_create(__MODULE__, event)
        rescue
          # e -> Exception.throw_exception(e, event, :create)
          e -> nil
        end
      end

      def destroy(event) do
        try do
          do_destroy(__MODULE__, event)
        rescue
          # e -> Exception.throw_exception(e, event, :destroy)
          e -> nil
        end
      end

      def update(event) do
        try do
          do_update(__MODULE__, event)
        rescue
          # e -> Exception.throw_exception(e, event, :update)
          e -> nil
        end
      end

      def before_create(event_data), do: event_data
      def process_create(event), do: process_create(__MODULE__, event)
      def after_create(event_data), do: {:ok, event_data}
      def after_create_transaction(event_data), do: {:ok, event_data}
      def before_update(event_data), do: event_data
      def process_update(event), do: process_update(__MODULE__, event)
      def after_update(event_data), do: {:ok, event_data}
      def after_update_transaction(event_data), do: {:ok, event_data}
      def before_destroy(event_data), do: event_data
      def process_destroy(event), do: process_destroy(__MODULE__, event)
      def after_destroy(event_data), do: {:ok, event_data}

      defoverridable after_create: 1,
                     before_create: 1,
                     before_destroy: 1,
                     after_create_transaction: 1,
                     after_destroy: 1,
                     after_update: 1,
                     after_update_transaction: 1,
                     before_update: 1,
                     create: 1,
                     destroy: 1,
                     update: 1,
                     process_create: 1,
                     process_destroy: 1,
                     process_update: 1
    end
  end

  defmodule Helpers do
    import Ecto.Query
    require Logger
    require Record
    alias Procon.Schemas.Ecto.ProconConsumerIndex
    @indexes_ets_table :procon_consumer_indexes

    def ets_table, do: @indexes_ets_table

    def do_create(controller, event) do
      if message_not_already_processed?(controller, event) do
        {:ok, {final_event_data, consumer_message_index}} =
          controller.datastore.transaction(fn ->
            controller.process_create(event)
            |> case do
              {:ok, event_data} ->
                {:ok, final_event_data} = controller.after_create(event_data)
                consumer_message_index = update_consumer_message_index(controller, event)
                {final_event_data, consumer_message_index}

              {:error, ecto_changeset} ->
                Logger.warn(
                  "Unable to create #{inspect(controller.model)} with event #{inspect(event)}@@ changeset : #{
                    inspect(ecto_changeset)
                  }"
                )

                controller.datastore.rollback(ecto_changeset)
            end
          end)

        update_consumer_message_index_ets(consumer_message_index)
        controller.after_create_transaction(final_event_data)
      end

      Wok.Message.noreply(event)
    end

    def process_create(controller, event) do
      event_data =
        record_and_body_from_event(controller, event)
        |> event_data_with_attributes(controller.keys_mapping)
        |> controller.before_create()

      controller.model.create_changeset(event_data.record, event_data.attributes)
      |> controller.datastore.insert_or_update()
      |> case do
        {:ok, struct} -> {:ok, Map.put(event_data, :record, struct)}
        {:error, ecto_changeset} -> {:error, ecto_changeset}
      end
    end

    def do_update(controller, event) do
      if message_not_already_processed?(controller, event) do
        {:ok, {final_event_data, consumer_message_index}} =
          controller.datastore.transaction(fn ->
            controller.process_update(event)
            |> case do
              {:ok, event_data} ->
                {:ok, final_event_data} = controller.after_update(event_data)
                consumer_message_index = update_consumer_message_index(controller, event)
                {final_event_data, consumer_message_index}

              {:error, ecto_changeset} ->
                Logger.warn(
                  "Unable to update #{inspect(controller.model)} with event #{inspect(event)}@@ changeset : #{
                    inspect(ecto_changeset)
                  }"
                )

                controller.datastore.rollback(ecto_changeset)
            end
          end)

        update_consumer_message_index_ets(consumer_message_index)
        controller.after_update_transaction(final_event_data)
      end

      Wok.Message.noreply(event)
    end

    def process_update(controller, event) do
      event_data =
        record_and_body_from_event(controller, event)
        |> event_data_with_attributes(controller.keys_mapping)
        |> controller.before_update()

      controller.model.update_changeset(event_data.record, event_data.attributes)
      |> controller.datastore.insert_or_update()
      |> case do
        {:ok, struct} -> {:ok, Map.put(event_data, :record, struct)}
        {:error, ecto_changeset} -> {:error, ecto_changeset}
      end
    end

    def event_data_with_attributes(event_data, atom_changeset) do
      attributes =
        for {key, val} <- event_data.payload, into: %{} do
          {Map.get(atom_changeset, key, String.to_atom(key)), val}
        end

      Map.put(event_data, :attributes, attributes)
    end

    def do_destroy(controller, event) do
      if message_not_already_processed?(controller, event) do
        {:ok, consumer_message_index} =
          controller.datastore.transaction(fn ->
            controller.process_destroy(event)
            |> case do
              {:ok, event_data} ->
                {:ok, _} = controller.after_destroy(event_data)
                update_consumer_message_index(controller, event)

              {:error, ecto_changeset} ->
                Logger.info(
                  "Unable to destroy #{inspect(controller.model)} in event #{inspect(event)}@@#{
                    inspect(ecto_changeset)
                  }"
                )

                controller.datastore.rollback(ecto_changeset)
            end
          end)

        update_consumer_message_index_ets(consumer_message_index)
      end

      Wok.Message.noreply(event)
    end

    def process_destroy(controller, event) do
      event_data =
        record_and_body_from_event(controller, event, false)
        |> controller.before_destroy()

      case event_data.record do
        nil ->
          Logger.warn(
            "No match for destroyed #{inspect(controller.model)} in event #{inspect(event)}"
          )

          {:ok, event_data}

        _ ->
          Logger.info("Destroying #{inspect(controller.model)} with id #{event_data.record.id}")

          case controller.datastore.delete(event_data.record) do
            {:ok, struct} -> {:ok, Map.put(event_data, :record, struct)}
            {:error, ecto_changeset} -> {:error, ecto_changeset}
          end
      end
    end

    def message_not_already_processed?(controller, event) do
      event.message_id > find_last_processed_message_id(controller, event)
    end

    def update_consumer_message_index_ets(consumer_message_index) do
      true =
        :ets.insert(
          @indexes_ets_table,
          {build_ets_key(consumer_message_index), consumer_message_index}
        )
    end

    defp get_consumer_message_index_ets(ets_key) do
      [{^ets_key, struct}] = :ets.lookup(@indexes_ets_table, ets_key)
      struct
    end

    def update_consumer_message_index(controller, event) do
      topic = Wok.Message.topic(event)
      partition = Wok.Message.partition(event)
      from = Wok.Message.from(event)
      ets_key = build_ets_key(from, topic, partition)
      message_id = Wok.Message.headers(event).message_id
      struct = get_consumer_message_index_ets(ets_key)

      case ConsumerMessageIndex.changeset(struct, %{message_id: message_id})
           |> controller.datastore.update() do
        {:ok, updated_struct} ->
          updated_struct

        {:error, ecto_changeset} ->
          Logger.info(
            "Unable to update message index #{inspect(ecto_changeset)} with id #{struct.id}"
          )

          controller.datastore.rollback(ecto_changeset)
      end
    end

    def build_ets_key(from, topic, partition), do: "#{from}_#{topic}_#{partition}"

    def build_ets_key(consumer_message_index) do
      "#{consumer_message_index.from}_#{consumer_message_index.topic}_#{
        consumer_message_index.partition
      }"
    end

    defp find_last_processed_message_id(controller, event) do
      topic = Wok.Message.topic(event)
      partition = Wok.Message.partition(event)
      from = Wok.Message.from(event)
      ets_key = build_ets_key(from, topic, partition)

      last_processed_message_id =
        case :ets.lookup(@indexes_ets_table, ets_key) do
          [] ->
            case from(c in ConsumerMessageIndex,
                   where: [from: ^from, topic: ^topic, partition: ^partition]
                 )
                 |> controller.datastore.one do
              nil ->
                struct =
                  controller.datastore.insert!(%ProconConsumerIndex{
                    from: from,
                    partition: partition,
                    topic: topic,
                    message_id: -1
                  })

                update_consumer_message_index_ets(struct)
                struct.message_id

              struct ->
                update_consumer_message_index_ets(struct)
                struct.message_id
            end

          [{^ets_key, struct}] ->
            struct.message_id
        end

      last_processed_message_id
    end

    def record_and_body_from_event(controller, event, return_record \\ true) do
      body = expected_version_of_body(event, controller.message_version)
      payload = Map.get(body, "payload", :no_payload)

      if !is_nil(controller.master_key) do
        controller.datastore.get_by(
          controller.model,
          [{elem(controller.master_key, 0), payload[elem(controller.master_key, 1)]}]
        )
      else
        controller.datastore.get(controller.model, payload["id"])
      end
      |> case do
        nil ->
          %{
            body: body,
            payload: payload,
            record: if(return_record, do: struct(controller.model), else: nil)
          }

        struct ->
          %{body: body, payload: payload, record: struct}
      end
    end

    def expected_version_of_body(message, version) do
      Wok.Message.body(message)
      |> log_message(Wok.Message.to(message))
      |> Poison.decode!()
      |> Enum.find(&(&1["version"] == version))
    end

    defp log_message(body, to) do
      Logger.info("Message received: #{to} #{body}")
      body
    end
  end
end
