defmodule Mix.Tasks.Procon.Helpers.DefaultUpdater do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def create_default_update_service(
        domain_services_directory_path,
        processor_name,
        processor_repo
      ) do
    file_path = Path.join([domain_services_directory_path, "updater.ex"])

    unless File.exists?(file_path) do
      create_file(
        file_path,
        default_service_updater_template(
          module: Helpers.default_service_name(processor_name) <> ".Updater",
          processor_module: processor_name,
          processor_repo_module: Helpers.repo_name_to_module(processor_name, processor_repo),
          schema_module: Helpers.default_schema_module(processor_name),
          serializer_module: Helpers.default_serializer_module(processor_name)
        )
      )
    end
  end

  embed_template(
    :default_service_updater,
    """
    defmodule <%= @module %> do
      # alias <%= @processor_module %>, as: Processor
      alias <%= @schema_module %>, as: Entity
      alias <%= @processor_repo_module %>, as: Repo

      def update(id, %{} = attributes, %{} = _assigns) do
        Ecto.Multi.new()
        |> Ecto.Multi.run(:entity, fn _repo, _changes ->
          Repo.get(Entity, id)
          |> case do
            nil ->
              {:error, :not_found}
            entity ->
              {:ok, entity}
          end
        end)
        |> Ecto.Multi.update(:updated_entity, fn %{entity: entity} ->
           Entity.api_update_changeset(entity, attributes)
        end)
        |> Ecto.Multi.run(:created_message, fn _repo, %{updated_entity: entity} ->
          Procon.MessagesEnqueuers.Ecto.enqueue_event(
            entity,
            <%= @serializer_module %>,
            :updated
          )

          {:ok, nil}
        end)
        |> <%= @processor_repo_module %>.transaction()
        |> case do
          {:ok, %{updated_entity: updated_entity}} ->
            Procon.MessagesProducers.ProducersStarter.start_topic_production(
              <%= @serializer_module %>
            )

            {:ok, updated_entity}

          {:error, :entity, :not_found, _changes} ->
            {:error, :entity, :not_found}

          {:error, :updated_entity, changeset, _changes} ->
            {:error, :updated_entity, changeset}
        end
      end
    end
    """
  )
end
