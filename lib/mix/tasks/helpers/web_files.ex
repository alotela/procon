defmodule Mix.Tasks.Procon.Helpers.WebDirectory do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def generate_web_directory(app_web, processor_name, processor_path, crud) do
    web_path = Path.join([processor_path, "web"])

    unless File.exists?(web_path) do
      Helpers.info("creating web directory #{web_path}")
      create_directory(web_path)
    end

    controllers_path = Path.join([web_path, "controllers"])

    unless File.exists?(controllers_path) do
      Helpers.info("creating web controllers directory #{controllers_path}")
      create_directory(controllers_path)

      Helpers.DefaultController.create_default_controller(
        processor_name,
        controllers_path,
        app_web,
        crud
      )
    end

    home_controller_path = Path.join([controllers_path, "home.ex"])

    unless File.exists?(home_controller_path) do
      Helpers.info("creating web home controller file #{home_controller_path}")

      create_file(
        home_controller_path,
        home_controller_template(processor_name: processor_name)
      )
    end

    templates_path = Path.join([web_path, "templates"])

    unless File.exists?(templates_path) do
      Helpers.info("creating templates directory #{templates_path}")
      create_directory(templates_path)
    end

    layout_path = Path.join([templates_path, "layout"])

    unless File.exists?(layout_path) do
      Helpers.info("creating layout directory #{layout_path}")
      create_directory(layout_path)
    end

    app_layout_path = Path.join([layout_path, "app.html.eex"])

    unless File.exists?(app_layout_path) do
      Helpers.info("creating layout app file #{app_layout_path}")

      create_file(
        app_layout_path,
        app_layout_template(
          processor_name: processor_name,
          render_call: "<%= render @view_module, @view_template, assigns %>"
        )
      )
    end

    home_directory_path = Path.join([templates_path, "home"])

    unless File.exists?(home_directory_path) do
      Helpers.info("creating home templates directory #{home_directory_path}")
      create_directory(home_directory_path)
    end

    home_template_path = Path.join([home_directory_path, "show.html.eex"])

    unless File.exists?(home_template_path) do
      Helpers.info("creating home template file #{home_template_path}")

      create_file(
        home_template_path,
        home_template_template(processor_name: processor_name)
      )
    end

    views_path = Path.join([web_path, "views"])

    unless File.exists?(views_path) do
      Helpers.info("creating web views path #{views_path}")
      create_directory(views_path)
    end

    layout_view_path = Path.join([views_path, "layout_view.ex"])

    unless File.exists?(layout_view_path) do
      Helpers.info("creating layout view file #{layout_view_path}")

      create_file(
        layout_view_path,
        layout_view_template(processor_name: processor_name)
      )
    end

    home_view_path = Path.join([views_path, "home_view.ex"])

    unless File.exists?(home_view_path) do
      Helpers.info("creating home view file #{home_view_path}")

      create_file(
        home_view_path,
        home_view_template(processor_name: processor_name)
      )
    end

    entity_view_path =
      Path.join([
        views_path,
        "#{processor_name |> Helpers.processor_to_resource()}_view.ex"
      ])

    unless File.exists?(entity_view_path) do
      Helpers.info("creating entity view file #{entity_view_path}")

      create_file(
        entity_view_path,
        entity_view_template(
          controller: processor_name |> Helpers.processor_to_controller(),
          processor_name: processor_name
        )
      )
    end

    router_path = Path.join([web_path, "router.ex"])

    unless File.exists?(router_path) do
      Helpers.info("creating router file #{router_path}")

      actions =
        Enum.reduce(
          [["c", :create], ["r", :show], ["u", :update], ["i", :index], ["d", :delete]],
          [],
          fn [letter, action], acc ->
            case String.contains?(crud, letter) do
              true ->
                [":#{action}" | acc]

              false ->
                acc
            end
          end
        )

      create_file(
        router_path,
        router_template(
          actions: actions |> Enum.join(", "),
          app_web: app_web,
          processor_name: processor_name,
          controller: processor_name |> Helpers.processor_to_controller(),
          resource_path: "/#{processor_name |> Helpers.processor_to_resource()}"
        )
      )
    end
  end

  embed_template(
    :router,
    """
    defmodule <%= @processor_name%>.Web.Router do
      use <%= @app_web%>, :router

      scope "/", <%= @processor_name %>.Web.Controllers do
        pipe_through(:browser)

        get("/", Home, :show, singleton: true)
      end

      scope "/api", <%= @processor_name %>.Web.Controllers, as: :api do
        pipe_through([:api, :jsonapi])
        resources("<%= @resource_path %>", <%= @controller %>, only: [<%= @actions %>])
      end
    end
    """
  )

  embed_template(
    :layout_view,
    """
    defmodule <%= @processor_name%>.Web.Views.Layout do
      use <%= @processor_name%>.Web, :view
    end
    """
  )

  embed_template(
    :entity_view,
    """
    defmodule <%= @processor_name %>.Web.Views.<%= @controller %> do
      use JaSerializer.PhoenixView
      attributes([:inserted_at, :updated_at])
    end
    """
  )

  embed_template(
    :home_view,
    """
    defmodule <%= @processor_name%>.Web.Views.Home do
      use <%= @processor_name%>.Web, :view
    end
    """
  )

  embed_template(:home_template, """
  <p>show page of <%= @processor_name %> processor</p>
  """)

  embed_template(
    :home_controller,
    """
    defmodule <%= @processor_name %>.Web.Controllers.Home do
      use <%= @processor_name %>.Web, :controller

      def show(conn, _params) do
        render(conn, "show.html")
      end
    end
    """
  )

  embed_template(:app_layout, """
  <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="utf-8"/>
      <meta http-equiv="X-UA-Compatible" content="IE=edge"/>
      <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
      <title><% @processor_name %> Processor · Phoenix Framework</title>
    </head>
      <body>
      <main role="main" class="container">
        <%= @render_call %>
      </main>
    </body>
  </html>
  """)
end
