defmodule Mix.Tasks.Procon.Helpers.ControllersErrors do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def create_default_controllers_errors(app_web) do
    file_path = ["lib", Macro.underscore(app_web), "controllers", "errors.ex"] |> Path.join()
    Helpers.info("creating controllers errors file #{file_path}")

    unless File.exists?(file_path) do
      create_file(file_path, t_template(app_web: app_web))
    end

    file_path
  end

  def create_controllers_helpers(app_web) do
    file_path = ["lib", Macro.underscore(app_web), "controllers", "helpers.ex"] |> Path.join()
    Helpers.info("creating controllers errors file #{file_path}")

    unless File.exists?(file_path) do
      create_file(file_path, helpers_template(app_web: app_web))
    end

    file_path
  end

  embed_template(
    :t,
    """
    defmodule <%= @app_web %>.Controllers.Errors do
      def error_to_http_code(error) do
        case error do
         :created_entity -> 422
         :updated_entity -> 422
         :deleted_entity -> 422
         :deleted_entities -> 422
         :entity -> 404
        end
      end

      def error(status_code, id, opts \\\\ [])

      def error(422, changeset, _opts), do: changeset
    end
    """
  )

  embed_template(
    :helpers,
    """
    defmodule <%= @app_web %>.Controllers.Helpers do
      use <%= @app_web %>, :controller

      def render_error(conn, status, error_id, data \\\\ []) do
        conn
        |> put_status(status)
        |> put_view(<%= @app_web %>.ApiErrorView)
        |> Phoenix.Controller.render(
          :errors,
          data: <%= @app_web %>.Controllers.Errors.error(status, error_id, data)
        )
        |> halt()
      end
    end
    """
  )
end
