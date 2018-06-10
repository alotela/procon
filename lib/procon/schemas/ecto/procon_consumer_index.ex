defmodule Procon.Schemas.Ecto.ProconConsumerIndex do
  use Ecto.Schema

  import Ecto.Changeset

  @type t :: %__MODULE__{}

  @accepted_params ~w(from message_id partition topic)a
  @required_params ~w(from message_id partition topic)a

  schema "procon_consumer_indexes" do
    field :from, :string
    field :message_id, :integer
    field :partition, :integer
    field :topic, :string
    timestamps
  end

  def changeset(record, params \\ :invalid) do
    record
    |> cast(params, @accepted_params)
    |> validate_required(@required_params)
  end
end
