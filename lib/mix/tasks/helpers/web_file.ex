defmodule Mix.Tasks.Procon.Helpers.WebFile do
  import Mix.Generator

  def generate_web_file(app_web, processor_name, processor_path) do
    web_file_path = Path.join([processor_path, "web.ex"])

    unless File.exists?(web_file_path) do
      create_file(
        web_file_path,
        web_file_template(
          app_web: app_web,
          processor_name: processor_name,
          processor_path: processor_path
        )
      )
    end
  end

  embed_template(
    :web_file,
    """
    defmodule <%= @processor_name %>.Web do
      def controller do
        quote do
          use Phoenix.Controller, namespace: <%= @processor_name %>.Web

          import Plug.Conn
          import <%= @app_web %>.Gettext
          alias <%= @app_web %>.Router.Helpers, as: Routes
          plug :put_layout, {<%= @processor_name%>.Web.Views.Layout, "app.html"}
          plug :put_view, Procon.PhoenixWebHelpers.module_to_view(__MODULE__)
        end
      end

      def view do
        quote do
          use Phoenix.View,
            path: __MODULE__ |> Module.split() |> List.last() |> String.downcase(),
            root: "<%= @processor_path %>/web/templates",
            namespace: <%= @processor_name %>.Web

          # Import convenience functions from controllers
          import Phoenix.Controller, only: [get_flash: 1, get_flash: 2, view_module: 1]

          # Use all HTML functionality (forms, tags, etc)
          use Phoenix.HTML

          import <%= @app_web %>.ErrorHelpers
          import <%= @app_web %>.Gettext
          alias <%= @app_web %>.Router.Helpers, as: Routes
        end
      end

      defmacro __using__(which) when is_atom(which) do
        apply(__MODULE__, which, [])
      end
    end
    """
  )
end
