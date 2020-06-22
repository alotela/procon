defmodule Procon.Services.DynamicTopics.Creator do
  alias Procon.Schemas.DynamicTopic, as: Entity

  @spec create(%{}, dynamic_topics_serializer: module()) :: {:error, any} | {:ok, any}
  def create(attributes, options \\ []) do
    entity =
      attributes
      |> Entity.api_create_changeset()
      |> Ecto.Changeset.apply_changes()

    real_entity = %Entity{
      entity
      | inserted_at: DateTime.utc_now()
    }

    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.run(:create_topic, fn _repo, _data ->
        Procon.Topics.create_topic(real_entity.topic_name, real_entity.partitions_count)
        {:ok, nil}
      end)
      |> Ecto.Multi.run(:message, fn _repo, _data ->
        Procon.MessagesEnqueuers.Ecto.enqueue_event(
          real_entity,
          Keyword.fetch!(options, :dynamic_topics_serializer),
          :created
        )

        {:ok, nil}
      end)

    case Keyword.get(options, :return_multi, nil) do
      nil ->
        multi
        |> Keyword.fetch!(options, :dynamic_topics_serializer).repo().transaction()
        |> case do
          {:ok, data} ->
            Procon.MessagesProducers.ProducersStarter.start_topic_production(
              Keyword.fetch!(options, :dynamic_topics_serializer)
            )

            {:ok, data}

          {:error, _, error, _changes} ->
            {:error, error}
        end

      _ ->
        multi
    end
  end

  def start_topic_production(options \\ []) do
    Procon.MessagesProducers.ProducersStarter.start_topic_production(
      Keyword.fetch!(options, :dynamic_topics_serializer)
    )
  end
end
