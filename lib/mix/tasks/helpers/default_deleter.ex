defmodule Mix.Tasks.Procon.Helpers.DefaultDeleter do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def create_default_delete_service(domain_services_directory_path, processor_name, processor_repo) do
    file_path = Path.join([domain_services_directory_path, "deleter.ex"])

    unless File.exists?(file_path) do
      create_file(
        file_path,
        default_service_deleter_template(
          module: Helpers.default_service_name(processor_name) <> ".Deleter",
          processor_repo_module: Helpers.repo_name_to_module(processor_name, processor_repo),
          schema_module: Helpers.default_schema_module(processor_name),
          serializer_module: Helpers.default_serializer_module(processor_name)
        )
      )
    end
  end

  embed_template(
    :default_service_deleter,
    """
    defmodule <%= @module %> do
      import Ecto.Query

      def delete(id, _options \\\\ []) do
        Ecto.Multi.new()
        |> Ecto.Multi.delete_all(:deleted_entities, from(e in <%= @schema_module %>, select: e, where: e.id == ^id))
        |> Ecto.Multi.run(:deleted_message, fn _repo, %{deleted_entities: {1, [deleted_entity]}} ->
          deleted_entity
          |> Procon.MessagesEnqueuers.Ecto.enqueue_event(<%= @serializer_module %>, :deleted)
        end)
        |> <%= @processor_repo_module %>.transaction()
        |> case do
          {:ok, %{deleted_entities: {1, [deleted_entity]}}} ->
            Procon.MessagesProducers.ProducersStarter.start_topic_production(<%= @serializer_module %>)
            {:ok, deleted_entity}

          {:error, :deleted_entities, changeset, _changes} ->
            {:error, :deleted_entities, changeset}
        end
      end
    end
    """
  )
end
