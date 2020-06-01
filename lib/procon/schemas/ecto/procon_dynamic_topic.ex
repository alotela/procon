defmodule Procon.Schemas.DynamicTopic do
  use Ecto.Schema
  import Ecto.Changeset

  @api_create_cast_attributes [
    :entity,
    :inserted_at,
    :partitions_count,
    :processor,
    :tenant_id,
    :topic_name
  ]
  @api_create_required_attributes @api_create_cast_attributes
  @messages_create_cast_attributes [
    :entity,
    :inserted_at,
    :partitions_count,
    :processor,
    :tenant_id,
    :topic_name
  ]
  @messages_create_required_attributes @messages_create_cast_attributes

  schema "procon_dynamic_topics" do
    field(:entity, :string)
    field(:inserted_at, :naive_datetime)
    field(:partitions_count, :integer)
    field(:processor, :string)
    field(:tenant_id, Ecto.UUID)
    field(:topic_name, :string)
  end

  def api_create_changeset(entity \\ __struct__(), attributes) do
    entity
    |> cast(attributes, @api_create_cast_attributes)
    |> validate_required(@api_create_required_attributes)
  end

  def messages_create_changeset(entity \\ __struct__(), attributes) do
    entity
    |> cast(attributes, @messages_create_cast_attributes)
    |> validate_required(@messages_create_required_attributes)
  end
end
