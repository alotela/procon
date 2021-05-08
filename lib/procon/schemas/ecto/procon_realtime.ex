defmodule Procon.Schemas.ProconRealtime do
  use Ecto.Schema

  schema "procon_realtimes" do
    field(:channel, :string)
    field(:metadata, :map)
    field(:session_id, :string)
  end
end
