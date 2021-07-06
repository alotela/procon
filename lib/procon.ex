defmodule Procon do
  defmodule Types do
    @type ulid() :: binary()
    @type uuid() :: binary()

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

      @enforce_keys [:transaction]
      defstruct after: nil,
                before: nil,
                transaction: %{id: 0}

      @type event() :: %{
              optional(:id) => Procon.Types.ulid() | Procon.Types.uuid(),
              optional(:metadata) => Procon.Types.Metadata.t(),
              optional(:inserted_at) => String.t(),
              optional(:updated_at) => String.t(),
              optional(any) => any
            }

      @typedoc "A debezium message"
      @type t() :: %__MODULE__{
              after: nil | event(),
              before: nil | event(),
              transaction: %{id: integer}
            }
    end
  end
end
