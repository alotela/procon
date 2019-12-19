defmodule Mix.Tasks.Procon.Helpers.DefaultSchema do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def create_default_schema(processor_name, schema_directory_path) do
    schema_file_path = Path.join([
      schema_directory_path,
      processor_name
      |> Helpers.processor_to_resource()
      |> Inflex.singularize()
      |> Kernel.<>(".ex")
    ])

    unless File.exists?(schema_file_path) do
      create_file(
        schema_file_path,
        default_entity_schema_template(
          module: Helpers.default_schema_module(processor_name),
          table: Helpers.processor_to_resource(processor_name)
        )
      )
    end
  end

  embed_template(
    :default_entity_schema,
    """
    defmodule <%= @module %> do
      use Ecto.Schema
      import Ecto.Changeset
      @primary_key {:id, :binary_id, autogenerate: true}
      @derive {Phoenix.Param, key: :id}
      @foreign_key_type Ecto.UUID
      @api_create_cast_attributes []
      @api_create_required_attributes []
      @api_update_cast_attributes []
      @api_update_required_attributes []

      schema "<%= @table %>" do

        timestamps()
      end

      def api_create_changeset(entity \\\\ __struct__(), attributes) do
        entity
        |> cast(attributes, @api_create_cast_attributes)
        |> validate_required(@api_create_required_attributes)
      end

      def api_update_changeset(entity, attributes) do
        entity
        |> cast(attributes, @api_update_cast_attributes)
        |> validate_required(@api_update_required_attributes)
      end
    end
    """
  )
end
