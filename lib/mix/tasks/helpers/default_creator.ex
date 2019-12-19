defmodule Mix.Tasks.Procon.Helpers.DefaultCreator do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def create_default_create_service(domain_services_directory_path, processor_name, processor_repo) do
    file_path = Path.join([domain_services_directory_path, "creator.ex"])

    unless File.exists?(file_path) do
      create_file(
        file_path,
        default_service_creator_template(
          creator_module: Helpers.default_service_name(processor_name) <> ".Creator",
          processor_module: processor_name,
          processor_repo_module: Helpers.repo_name_to_module(processor_name, processor_repo),
          schema_module: Helpers.default_schema_module(processor_name),
          serializer_module: Helpers.default_serializer_module(processor_name)
        )
      )
    end
  end

  embed_template(
    :default_service_creator,
    """
    defmodule <%= @creator_module %> do
      # alias <%= @processor_module %>, as: Processor

      def create(%{} = attributes) do
        Ecto.Multi.new()
        |> Ecto.Multi.insert(
          :created_entity,
          attributes
          |> <%= @schema_module %>.api_create_changeset(),
          returning: true
        )
        |> Ecto.Multi.run(:created_message, fn _repo, %{created_entity: entity} ->
          Procon.MessagesEnqueuers.Ecto.enqueue_event(
            entity,
            <%= @serializer_module %>,
            :created
          )

          {:ok, nil}
        end)
        |> <%= @processor_repo_module %>.transaction()
        |> case do
          {:ok, %{created_entity: created_entity}} ->
            Procon.MessagesProducers.ProducersStarter.start_topic_production(
              <%= @serializer_module %>
            )

            {:ok, created_entity}

          {:error, :created_entity, changeset, _changes} ->
            {:error, :created_entity, changeset}
        end
      end
    end
    """
  )
end
