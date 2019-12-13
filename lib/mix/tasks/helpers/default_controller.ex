defmodule Mix.Tasks.Procon.Helpers.DefaultController do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def create_default_controller(processor_name, controllers_path) do
    controller_file_path = Path.join([controllers_path, Helpers.processor_to_resource(processor_name) <> ".ex"])

    unless File.exists?(controller_file_path) do
      create_file(
        controller_file_path,
        default_controller_template(
          controller_module: Helpers.default_controller_module(processor_name),
          default_service_creator_module: Helpers.default_service_name(processor_name) <> ".Creator",
          processor_module: processor_name,
          resources: Helpers.processor_to_resource(processor_name)
        )
      )
    end

  end

  embed_template(
    :default_controller,
    """
    defmodule <%= @controller_module %> do
      alias <%= @processor_module %>, as: Processor
      use Processor.Web, :controller

      def create(conn, %{"data" => %{"attributes" => attributes, "type" => "<%= @resources %>"}}) do
        attributes
        |> <%= @default_service_creator_module %>.create()
        |> case do
          {:error, :created_entity, changeset} ->
            CalionsWeb.Controllers.Helpers.render_error(conn, 422, changeset)

          {:ok, created_entity} ->
            conn
            |> put_status(:created)
            |> render("show.json-api", data: created_entity)
        end
      end
    end
    """
  )
end
