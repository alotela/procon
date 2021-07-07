defmodule Procon do
  defmodule Types do
    @type ulid() :: binary()
    @type uuid() :: binary()

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
        :keys_mapping,
        :master_key,
        :model,
        :offset,
        :partition,
        :processing_id,
        :processor_name,
        :serialization,
        :topic
      ]

      @typep ecto_repo() :: atom()

      @typedoc "options passed to a base controller's method."
      @type t() :: %__MODULE__{
              avro_key_schema_version: non_neg_integer(),
              avro_value_schema_version: non_neg_integer(),
              bypass_exception: boolean(),
              datastore: ecto_repo(),
              drop_event_attributes: map(),
              dynamic_topics_autostart_consumers: boolean,
              dynamic_topics_filters: list(),
              keys_mapping: map(),
              master_key: nil | atom(),
              model: module(),
              offset: non_neg_integer(),
              partition: non_neg_integer(),
              processing_id: non_neg_integer(),
              processor_name: String.t(),
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
              keys_mapping: map(),
              master_key: nil | atom(),
              model: module(),
              offset: non_neg_integer(),
              partition: non_neg_integer(),
              processing_id: non_neg_integer(),
              processor_name: String.t(),
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
