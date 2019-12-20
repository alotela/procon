defmodule Mix.Tasks.Procon.Helpers.DefaultGetter do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def create_default_getter_service(services_path, processor_name, processor_repo, get_search) do
    file_path = Path.join([services_path, "getter.ex"])

    unless File.exists?(file_path) do
      create_file(
        file_path,
        default_template(
          module: Helpers.default_service_name(processor_name),
          processor_repo_module: Helpers.repo_name_to_module(processor_name, processor_repo),
          entity_module: Helpers.default_schema_module(processor_name),
          get_entity: String.contains?(get_search, "r"),
          get_entities: String.contains?(get_search, "i")
        )
      )
    end
  end

  embed_template(
    :default,
    """
    defmodule <%= @module %>.Getter do
      alias <%= @entity_module %>, as: Entity
      alias <%= @processor_repo_module %>, as: Repo

      def get(a, options \\\\ [])

    <%= if @get_entity do %>
      def get(id, _options) when is_binary(id) or is_integer(id) do
        Repo.get(Entity, id)
      end
    <% end %>

    <%= if @get_entities do %>
      def get(params, _options) when is_map(params) do
        Repo.all(Entity)
      end
    <% end %>
    end
    """
  )
end
