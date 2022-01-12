defmodule Procon do
  defmodule Types do
    @type procon_db_actions() :: :created | :updated
    @type procon_entity() :: struct()
    @type ulid() :: binary()
    @type uuid() :: binary()

    defmodule EventData do
    end

    defmodule BaseMethodOptions do
      @moduledoc """
      options passed to a base controller's method.
      """
      defstruct [
        :avro_key_schema_version,
        :avro_value_schema_version,
        :bypass_exception,
        :datastore,
        :drop_event_attributes,
        :dynamic_topics_autostart_consumers,
        :dynamic_topics_filters,
        :ets_table_name,
        :keys_mapping,
        :master_key,
        :materialize_json_attributes,
        :model,
        :offset,
        :partition,
        :processing_id,
        :processor_name,
        :processor_type,
        :realtime_builder,
        :serialization,
        :topic,
        messages_controller: nil,
        message_processed_hook: nil
      ]

      @typep ecto_repo() :: atom()
      @typep event_key() :: atom()
      @typep target_key() :: atom()

      @typedoc "options passed to a base controller's method."
      @type t() :: %__MODULE__{
              avro_key_schema_version: non_neg_integer(),
              avro_value_schema_version: non_neg_integer(),
              bypass_exception: boolean(),
              datastore: ecto_repo(),
              drop_event_attributes: map(),
              dynamic_topics_autostart_consumers: boolean,
              dynamic_topics_filters: list(),
              ets_table_name: atom(),
              keys_mapping: map(),
              master_key: nil | {event_key(), target_key()},
              materialize_json_attributes: list(),
              messages_controller: nil | module(),
              message_processed_hook:
                nil
                | (Procon.Types.EventData.t(),
                   Procon.Types.procon_db_actions(),
                   Procon.Types.BaseMethodOptions ->
                     {:ok, Procon.Types.EventData.t()}),
              model: module(),
              offset: non_neg_integer(),
              partition: non_neg_integer(),
              processing_id: non_neg_integer(),
              processor_name: String.t(),
              processor_type: :command | :operator | :query,
              realtime_builder:
                (Procon.Types.EventData.t() ->
                   nil | Procon.Schemas.ProconRealtime.t()),
              serialization: :avro | :debezium,
              topic: String.t()
            }

      @type m() :: %{
              avro_key_schema_version: non_neg_integer(),
              avro_value_schema_version: non_neg_integer(),
              bypass_exception: boolean(),
              datastore: ecto_repo(),
              drop_event_attributes: map(),
              dynamic_topics_autostart_consumers: boolean,
              dynamic_topics_filters: list(),
              ets_table_name: atom(),
              keys_mapping: map(),
              master_key: nil | {event_key(), target_key()},
              materialize_json_attributes: list(),
              messages_controller: nil | module(),
              message_processed_hook:
                nil
                | (Procon.Types.EventData.t(),
                   Procon.Types.procon_db_actions(),
                   Procon.Types.BaseMethodOptions ->
                     {:ok, Procon.Types.EventData.t()}),
              model: module(),
              offset: non_neg_integer(),
              partition: non_neg_integer(),
              processing_id: non_neg_integer(),
              processor_name: String.t(),
              processor_type: :command | :operator | :query,
              realtime_builder:
                nil
                | (Procon.Types.EventData.t() ->
                     nil | Procon.Schemas.ProconRealtime.t()),
              serialization: :avro | :debezium,
              topic: String.t()
            }
    end

    defmodule Metadata do
      @moduledoc """
      A struct representing the metadata of a procon message.
      """
      defstruct [
        :account_id,
        :http_request_id,
        :session_token,
        :user_id
      ]

      @typedoc "The metadata part of a procon message"
      @type t() :: %__MODULE__{
              account_id: nil,
              http_request_id: Procon.Types.ulid(),
              session_token: Procon.Types.uuid(),
              user_id: Procon.Types.uuid()
            }

      @type m() :: %{
              account_id: nil,
              http_request_id: Procon.Types.ulid(),
              session_token: Procon.Types.uuid(),
              user_id: Procon.Types.uuid()
            }
    end

    defmodule DebeziumMessage do
      @moduledoc """
      A struct representing a debezium message.
      """

      defstruct after: nil,
                before: nil,
                transaction: nil

      @type event() :: %{
              optional(:id) => Procon.Types.ulid() | Procon.Types.uuid(),
              optional(:metadata) => Procon.Types.Metadata.m(),
              optional(:inserted_at) => String.t(),
              optional(:updated_at) => String.t(),
              optional(any) => any
            }

      @typedoc "A debezium message"
      @type t() :: %__MODULE__{
              after: nil | event(),
              before: nil | event(),
              transaction: nil | %{id: integer}
            }
    end
  end
end
