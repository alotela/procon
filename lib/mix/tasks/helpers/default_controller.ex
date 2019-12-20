defmodule Mix.Tasks.Procon.Helpers.DefaultController do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def create_default_controller(processor_name, controllers_path, app_web_module, crud) do
    controller_file_path = Path.join([controllers_path, Helpers.processor_to_resource(processor_name) <> ".ex"])

    unless File.exists?(controller_file_path) do
      create_file(
        controller_file_path,
        default_controller_template(
          controller_module: Helpers.default_controller_module(processor_name),
          app_web_module: app_web_module,
          default_service_module: Helpers.default_service_name(processor_name),
          processor_module: processor_name,
          resources: Helpers.processor_to_kebab_resource(processor_name),
          crud: crud
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

    <%= if String.contains?(@crud, "c") do %>
      def create(conn, %{"data" => %{"attributes" => attributes, "type" => "<%= @resources %>"}}) do
        attributes
        |> <%= @default_service_module %>.Creator.create()
        |> case do
          {:error, error_code, data} ->
            <%= @app_web_module %>.Controllers.Helpers.render_error(conn, <%= @app_web_module  %>.Controllers.Errors.error_to_http_code(error_code), data)

          {:ok, :accepted} ->
            conn |> send_resp(202, "")

          {:ok, :no_content} ->
            conn |> send_resp(204, "")

          {:ok, created_entity} ->
            conn
            |> put_status(:created)
            |> render("show.json-api", data: created_entity)
        end
      end
    <% end %>

    <%= if String.contains?(@crud, "d") do %>
      def delete(conn, %{"id" => id}) do
        <%= @default_service_module %>.Deleter.delete(id, conn.assigns)
        |> case do
          {:error, error_code, data} ->
            <%= @app_web_module %>.Controllers.Helpers.render_error(conn, <%= @app_web_module  %>.Controllers.Errors.error_to_http_code(error_code), data)

          {:ok, _} ->
            conn |> send_resp(204, "")
        end
      end
    <% end %>

    <%= if String.contains?(@crud, "i") do %>
      def index(conn, params) do
        conn
        |> put_status(:ok)
        |> render("index.json-api", data: <%= @default_service_module %>.Getter.get(params, conn.assigns))
      end
    <% end %>

    <%= if String.contains?(@crud, "r") do %>
      def show(conn, %{"id" => id}) do
        <%= @default_service_module %>.Getter.get(id, conn.assigns)
        |> case do
          {:error, error_code, data} ->
            <%= @app_web_module %>.Controllers.Helpers.render_error(conn, <%= @app_web_module %>.Controllers.Errors.error_to_http_code(error_code), data)

          {:ok, entity} ->
            conn
            |> put_status(:ok)
            |> render("show.json-api", data: entity)
        end
      end
    <% end %>

    <%= if String.contains?(@crud, "u") do %>
      def update(conn, %{"id" => id, "data" => %{"attributes" => attributes, "type" => "<%= @resources %>"}}) do
        <%= @default_service_module %>.Updater.update(id, attributes, conn.assigns)
        |> case do
          {:error, error_code, data} ->
            <%= @app_web_module %>.Controllers.Helpers.render_error(conn, <%= @app_web_module %>.Controllers.Errors.error_to_http_code(error_code), data)

          {:ok, :no_entity} ->
            conn |> send_resp(204, "")

          {:ok, updated_entity} ->
            conn
            |> put_status(:ok)
            |> render("show.json-api", data: updated_entity)
        end
      end
    <% end %>
    end
    """
  )
end
