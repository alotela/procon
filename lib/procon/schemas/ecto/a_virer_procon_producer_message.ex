defmodule Procon.Schemas.Ecto.ProconProducerMessage do
  use Ecto.Schema

  import Ecto.Changeset

  @type t :: %__MODULE__{}

  @accepted_params ~w(blob index partition topic)a
  @required_params ~w(blob index partition topic)a

  schema "procon_producer_messages" do
    field(:blob, :string)
    field(:index, :integer)
    field(:partition, :integer)
    field(:topic, :string)
    timestamps()
  end

  def changeset(record, params \\ :invalid) do
    record
    |> cast(params, @accepted_params)
    |> validate_required(@required_params)
  end
end
