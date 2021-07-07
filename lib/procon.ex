defmodule Procon do
  defmodule Types do
    @type ulid() :: binary()
    @type uuid() :: binary()

    defmodule BaseMethodOptions do
      @moduledoc """
      options passed to a base controller's method.
      """
      defstruct [
        :datastore,
        :dynamic_topics_autostart_consumers,
        :dynamic_topics_filters,
        :offset,
        :partition,
        :processing_id,
        :processor_name,
        :topic
      ]

      @typep ecto_repo() :: atom()

      @typedoc "options passed to a base controller's method."
      @type t() :: %__MODULE__{
              datastore: ecto_repo(),
              dynamic_topics_autostart_consumers: boolean,
              dynamic_topics_filters: list(),
              offset: non_neg_integer(),
              partition: non_neg_integer(),
              processing_id: non_neg_integer(),
              processor_name: String.t(),
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
              optional(:metadata) => map(),
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
