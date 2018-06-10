defmodule WokAsyncMessageHandler.Models.StoppedPartition do
  use Ecto.Schema

  import Ecto.Changeset

  @type t :: %__MODULE__{}

  @accepted_params ~w(topic partition message_id error)a
  @required_params ~w(topic partition message_id error)a

  schema "stopped_partitions" do
    field :topic, :string
    field :partition, :integer
    field :message_id, :integer
    field :error, :string
    timestamps
  end

  def create_changeset(record, params \\ :invalid) do
    record
    |> cast(params, @accepted_params)
    |> validate_required(@required_params)
  end

  def update_changeset(record, params \\ :invalid) do
    create_changeset(record, params)
  end
end
