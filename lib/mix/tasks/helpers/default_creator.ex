defmodule Mix.Tasks.Procon.Helpers.DefaultCreator do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def create_default_create_service(
        domain_services_directory_path,
        processor_name,
        processor_repo
      ) do
    file_path = Path.join([domain_services_directory_path, "creator.ex"])

    unless File.exists?(file_path) do
      create_file(
        file_path,
        default_service_creator_template(
          creator_module: Helpers.default_service_name(processor_name) <> ".Creator",
          processor_module: processor_name,
          processor_repo_module:
            Helpers.repo_name_to_module(processor_name, processor_repo)
            |> String.replace(processor_name, "Processor"),
          schema_module:
            Helpers.default_schema_module(processor_name)
            |> String.replace(processor_name, "Processor")
        )
      )
    end
  end

  embed_template(
    :default_service_creator,
    """
    defmodule <%= @creator_module %> do
      alias <%= @processor_module %>, as: Processor

      def create(%{} = attributes, metadata) do
        Ecto.Multi.new()
        |> Ecto.Multi.insert(
          :created_entity,
          attributes
          |> Map.put("metadata", metadata)
          |> <%= @schema_module %>.api_create_changeset(),
          returning: true
        )
        |> <%= @processor_repo_module %>.transaction()
        |> case do
          {:ok, %{created_entity: created_entity}} ->
            {:ok, created_entity}

          {:error, :created_entity, changeset, _changes} ->
            {:error, :created_entity, changeset}
        end
      end
    end
    """
  )
end
