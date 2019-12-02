defmodule Mix.Tasks.Procon.Init do
  use Mix.Task
  import Mix.Generator

  @shortdoc "Initialize Procon in your project"
  @moduledoc ~S"""
  #Usage
  ```
     mix procon.init --processor MyDomain.Processors.ProcessorName --repo ProcessorPg
  ```
  """

  def run([]) do
    Mix.Tasks.Procon.Helpers.info("You need to set --processor and --repo params :")

    Mix.Tasks.Procon.Helpers.info(
      "mix procon.init --processor MyDomain.Processors.ProcessorName --repo ProcessorPg"
    )
  end

  def run(args) do
    app_name = Mix.Project.config()[:app] |> to_string

    app_web =
      Mix.Project.get!() |> Module.split() |> List.first() |> to_string() |> Kernel.<>("Web")

    processor_name =
      OptionParser.parse(args, strict: [processor: :string, repo: :string])
      |> elem(0)
      |> Keyword.get(:processor)

    processor_repo =
      OptionParser.parse(args, strict: [repo: :string]) |> elem(0) |> Keyword.get(:repo)

    migrations_path = Path.join(["priv", processor_repo |> Macro.underscore(), "migrations"])

    Mix.Tasks.Procon.Helpers.info("creating migrations directory #{migrations_path}")
    create_directory(migrations_path)

    procon_producer_messages_migration =
      generate_migration(
        "procon_producer_messages",
        migrations_path,
        processor_name,
        processor_repo,
        &procon_producer_messages_template/1
      )

    procon_consumer_indexes_migration =
      generate_migration(
        "procon_consumer_indexes",
        migrations_path,
        processor_name,
        processor_repo,
        &procon_consumer_indexes_template/1
      )

    procon_producer_balancings_migration =
      generate_migration(
        "procon_producer_balancings",
        migrations_path,
        processor_name,
        processor_repo,
        &procon_producer_balancings_template/1
      )

    procon_producer_indexes_migration =
      generate_migration(
        "procon_producer_indexes",
        migrations_path,
        processor_name,
        processor_repo,
        &procon_producer_indexes_template/1
      )

    processor_path =
      Path.join([
        "lib",
        "processors",
        processor_name |> String.split(".") |> List.last() |> Macro.underscore()
      ])

    generate_repository(app_name, processor_name, processor_path, processor_repo)

    events_path = [processor_path, "events"] |> Path.join()

    Mix.Tasks.Procon.Helpers.info("creating processor's events directory #{events_path}")
    events_path |> create_directory()

    serializers_path = [events_path, "serializers"] |> Path.join()

    Mix.Tasks.Procon.Helpers.info(
      "creating processor's events serializers directory #{serializers_path}"
    )

    serializers_path |> create_directory()

    schemas_path = [processor_path, "schemas"] |> Path.join()

    Mix.Tasks.Procon.Helpers.info("creating schemas directory #{schemas_path}")
    schemas_path |> create_directory()

    services_path = [processor_path, "services"] |> Path.join()

    Mix.Tasks.Procon.Helpers.info("creating services directory #{services_path}")
    services_path |> create_directory()

    services_domain_path = [services_path, "domain"] |> Path.join()

    Mix.Tasks.Procon.Helpers.info("creating services directory #{services_domain_path}")
    services_domain_path |> create_directory()

    services_infra_path = [services_path, "domain"] |> Path.join()

    Mix.Tasks.Procon.Helpers.info("creating services directory #{services_infra_path}")
    services_infra_path |> create_directory()

    generate_web_directory(app_web, processor_name, processor_path)
    generate_web_file(app_web, processor_name, processor_path)

    generate_config_files(app_name, processor_name, processor_repo)

    msg = """
    Procon initialized for your project.

    Generated files and directories :
        #{procon_producer_messages_migration}: store messages to send to kafka (auto increment index for exactly once processing on consumer side)
        #{procon_consumer_indexes_migration}: store topic/partition consumed indexes (for exactly once processing)
        #{procon_producer_balancings_migration}: store which app/container produces which topic/partition
        #{procon_producer_indexes_migration}: store producers indexes for transactional information
        lib/events_serializers: directory where your events serializers will be generated
        lib/#{app_name}/message_controllers: directory where your messages controllers will be generated

    To finish the stup:
      add this line to lib/web/#{app_name}_web/router.ex:

      forward "/#{processor_name |> short_processor_name()}", #{processor_name}.Web.Router

      add these lines in ./config/config.exs :

      config :procon,
        brokers: [localhost: 9092],
        brod_client_config: [reconnect_cool_down_seconds: 10],
        nb_simultaneous_messages_to_send: 1000,
        offset_commit_interval_seconds: 5,
        consumers: []
      }

    generate serializers for your resources (data your service is master of and will generate events):
      mix procon.serializer --resource ResourceName


    Now you can produce message :

      Procon.MessagesEnqueuers.Ecto.enqueue_event(event_data, event_serializer, event_type)

    where :
      - event_data: map of your data for your message
      - event_status: :created or :updated or :deleted
      - event_serializer: module serializer you have generated and parameterized


    generate messages controller to consume events from kafka:
      mix procon.controller --schema SchemaName

    """

    Mix.Tasks.Procon.Helpers.info(msg)
  end

  def short_processor_name(processor_name) do
    processor_name |> String.split(".") |> List.last() |> Macro.underscore()
  end

  def generate_config_files(app_name, processor_name, processor_repo) do
    processors_config_directory = Path.join(["config", "processors"])

    unless File.exists?(processors_config_directory) do
      Mix.Tasks.Procon.Helpers.info(
        "creating processors config directory #{processors_config_directory}"
      )

      create_directory(processors_config_directory)
    end

    processor_config_directory =
      Path.join([processors_config_directory, short_processor_name(processor_name)])

    unless File.exists?(processor_config_directory) do
      Mix.Tasks.Procon.Helpers.info(
        "creating processor config directory #{processor_config_directory}"
      )

      create_directory(processor_config_directory)
    end

    processor_config_file = Path.join([processor_config_directory, "config.exs"])

    unless File.exists?(processor_config_file) do
      create_file(processor_config_file, processor_config_template([]))
    end

    dev_config_file = Path.join([processor_config_directory, "dev.exs"])

    unless File.exists?(dev_config_file) do
      create_file(
        dev_config_file,
        dev_config_template(
          app_name: app_name,
          repository:
            Mix.Tasks.Procon.Helpers.repo_name_to_module(processor_name, processor_repo)
            |> IO.inspect(label: "repositoryyyyyyy"),
          database: processor_name |> short_processor_name()
        )
      )
    end
  end

  embed_template(
    :processor_config,
    """
    use Mix.Config

    import_config "\#{Mix.env()}.exs"
    """
  )

  embed_template(
    :dev_config,
    """
    use Mix.Config

    config :<%= @app_name%>, <%= @repository %>,
    database: "<%= @database %>",
    hostname: "localhost",
    show_sensitive_data_on_connection_error: true,
    pool_size: 10
    """
  )

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

  def generate_web_directory(app_web, processor_name, processor_path) do
    web_path = Path.join([processor_path, "web"])

    unless File.exists?(web_path) do
      Mix.Tasks.Procon.Helpers.info("creating web directory #{web_path}")
      create_directory(web_path)
    end

    controllers_path = Path.join([web_path, "controllers"])

    unless File.exists?(controllers_path) do
      Mix.Tasks.Procon.Helpers.info("creating web controllers directory #{controllers_path}")
      create_directory(controllers_path)
    end

    home_controller_path = Path.join([controllers_path, "home.ex"])

    unless File.exists?(home_controller_path) do
      Mix.Tasks.Procon.Helpers.info("creating web home controller file #{home_controller_path}")

      create_file(
        home_controller_path,
        home_controller_template(processor_name: processor_name)
      )
    end

    templates_path = Path.join([web_path, "templates"])

    unless File.exists?(templates_path) do
      Mix.Tasks.Procon.Helpers.info("creating templates directory #{templates_path}")
      create_directory(templates_path)
    end

    layout_path = Path.join([templates_path, "layout"])

    unless File.exists?(layout_path) do
      Mix.Tasks.Procon.Helpers.info("creating layout directory #{layout_path}")
      create_directory(layout_path)
    end

    app_layout_path = Path.join([layout_path, "app.html.eex"])

    unless File.exists?(app_layout_path) do
      Mix.Tasks.Procon.Helpers.info("creating layout app file #{app_layout_path}")

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
      Mix.Tasks.Procon.Helpers.info("creating home templates directory #{home_directory_path}")
      create_directory(home_directory_path)
    end

    home_template_path = Path.join([home_directory_path, "show.html.eex"])

    unless File.exists?(home_template_path) do
      Mix.Tasks.Procon.Helpers.info("creating home template file #{home_template_path}")

      create_file(
        home_template_path,
        home_template_template(processor_name: processor_name)
      )
    end

    views_path = Path.join([web_path, "views"])

    unless File.exists?(views_path) do
      Mix.Tasks.Procon.Helpers.info("creating web views path #{views_path}")
      create_directory(views_path)
    end

    layout_view_path = Path.join([views_path, "layout_view.ex"])

    unless File.exists?(layout_view_path) do
      Mix.Tasks.Procon.Helpers.info("creating layout view file #{layout_view_path}")

      create_file(
        layout_view_path,
        layout_view_template(processor_name: processor_name)
      )
    end

    home_view_path = Path.join([views_path, "home_view.ex"])

    unless File.exists?(home_view_path) do
      Mix.Tasks.Procon.Helpers.info("creating home view file #{home_view_path}")

      create_file(
        home_view_path,
        home_view_template(processor_name: processor_name)
      )
    end

    router_path = Path.join([web_path, "router.ex"])

    unless File.exists?(router_path) do
      Mix.Tasks.Procon.Helpers.info("creating router file #{router_path}")

      create_file(
        router_path,
        router_template(processor_name: processor_name, app_web: app_web)
      )
    end
  end

  embed_template(
    :router,
    """
    defmodule <%= @processor_name%>.Web.Router do
      use <%= @app_web%>, :router

      pipeline :browser do
        plug(:accepts, ["html"])
        plug(:fetch_session)
        plug(:fetch_flash)
        plug(:protect_from_forgery)
        plug(:put_secure_browser_headers)
      end

      pipeline :api do
        plug(:accepts, ["multipart"])
      end

      scope "/", <%= @processor_name %>.Web.Controllers do
        pipe_through(:browser)

        get("/", Home, :show, singleton: true)
      end

      scope "/api", <%= @processor_name %>.Web.Controllers, as: :api do
        pipe_through([:api])
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

  def generate_repository(app_name, processor_name, processor_path, processor_repo) do
    repositories_path = Path.join([processor_path, "repositories"])
    Mix.Tasks.Procon.Helpers.info("creating repository path #{repositories_path}")
    create_directory(repositories_path)

    repo_file_path = Path.join([repositories_path, "pg.ex"])

    unless File.exists?(repo_file_path) do
      Mix.Tasks.Procon.Helpers.info("creating default repo file #{repo_file_path}")

      create_file(
        repo_file_path,
        pg_repo_template(
          app_name: app_name,
          processor_repo:
            Mix.Tasks.Procon.Helpers.repo_name_to_module(processor_name, processor_repo)
        )
      )
    end
  end

  embed_template(:pg_repo, """
  defmodule <%= @processor_repo %> do
    use Ecto.Repo,
        otp_app: :<%= @app_name %>,
        adapter: Ecto.Adapters.Postgres
  end
  """)

  defp generate_migration(
         filename,
         migrations_path,
         processor_name,
         processor_repo,
         template_function
       ) do
    unless file_exists?(migrations_path, "*_#{filename}.exs") do
      file = Path.join(migrations_path, "#{timestamp()}_#{filename}.exs")
      Mix.Tasks.Procon.Helpers.info("creating migration file #{file}")

      create_file(
        file,
        template_function.(
          processor_repo:
            Mix.Tasks.Procon.Helpers.repo_name_to_module(processor_name, processor_repo)
        )
      )

      file
    end
  end

  defp timestamp() do
    :timer.sleep(1000)
    {{y, m, d}, {hh, mm, ss}} = :calendar.universal_time()
    "#{y}#{pad(m)}#{pad(d)}#{pad(hh)}#{pad(mm)}#{pad(ss)}"
  end

  defp pad(i) when i < 10, do: <<?0, ?0 + i>>
  defp pad(i), do: to_string(i)

  embed_template(:procon_producer_messages, """
  defmodule <%= @processor_repo %>.Migrations.ProconProducerMessages do
    use Ecto.Migration

    def change do
      create table(:procon_producer_messages) do
        add :blob, :text, null: false
        add :is_stopped, :boolean
        add :partition, :integer, null: false
        add :stopped_error, :text
        add :stopped_message_id, :integer
        add :topic, :string, null: false
        timestamps
      end
      create index(:procon_producer_messages, [:is_stopped])
      create index(:procon_producer_messages, [:partition])
      create index(:procon_producer_messages, [:topic])
      alter table(:procon_producer_messages) do
        modify :id, :int8
      end
    end
  end
  """)

  embed_template(:procon_consumer_indexes, """
  defmodule <%= @processor_repo %>.Migrations.ProconMessageIndexes do
    use Ecto.Migration

    def change do
      create table(:procon_consumer_indexes) do
        add :message_id, :int8, null: false
        add :partition, :integer, null: false
        add :topic, :string, null: false
        add :error, :text, null: true
        timestamps
      end
      create index(:procon_consumer_indexes, [:partition])
      create index(:procon_consumer_indexes, [:topic])
    end
  end
  """)

  embed_template(:procon_producer_indexes, """
  defmodule <%= @processor_repo %>.Migrations.ProconProducerIndexes do
    use Ecto.Migration

    def change do
      create table(:procon_producer_indexes) do
        add :last_index, :int8, null: false
        add :partition, :integer, null: false
        add :topic, :string, null: false
      end
      create index(:procon_producer_indexes, [:partition])
      create index(:procon_producer_indexes, [:topic])
    end
  end
  """)

  embed_template(:procon_enqueur, """
  defmodule <%= @app_module %>.Procon.Enqueur do
    # this module is just an 'alias' to the real module
    # here we use Ecto, but you can use any other compatible strategy
    import Procon.MessagesEnqueuers.Ecto
  end

  """)

  embed_template(:procon_producer_balancings, """
  defmodule <%= @processor_repo %>.Migrations.ProconProducerBalancings do
    use Ecto.Migration

    def change do
      create table(:procon_producer_balancings) do
        add :id_producer, :integer
        add :topic, :string
        add :partition, :integer
        add :last_presence_at, :utc_datetime
      end
      create index(:procon_producer_balancings, [:partition])
      create index(:procon_producer_balancings, [:topic])
    end
  end
  """)

  defp file_exists?(migrations_path, globe) do
    [] != migrations_path |> Path.join(globe) |> Path.wildcard()
  end
end
