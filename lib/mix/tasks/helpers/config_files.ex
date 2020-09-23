defmodule Mix.Tasks.Procon.Helpers.ConfigFiles do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def generate_config_files(app_name, processor_name, processor_repo) do
    processors_config_type_directory =
      Path.join([
        "config",
        "processors",
        Helpers.processor_type(processor_name) |> Macro.underscore()
      ])

    unless File.exists?(processors_config_type_directory) do
      Helpers.info("creating processors config directory #{processors_config_type_directory}")

      create_directory(processors_config_type_directory)
    end

    processor_config_directory =
      Path.join([processors_config_type_directory, Helpers.short_processor_name(processor_name)])

    unless File.exists?(processor_config_directory) do
      Helpers.info("creating processor config directory #{processor_config_directory}")

      create_directory(processor_config_directory)
    end

    processor_config_file = Path.join([processor_config_directory, "config.exs"])

    unless File.exists?(processor_config_file) do
      create_file(
        processor_config_file,
        processor_config_template(
          processor_name: processor_name,
          repository: Helpers.repo_name_to_module(processor_name, processor_repo)
        )
      )
    end

    dev_config_file = Path.join([processor_config_directory, "dev.exs"])

    unless File.exists?(dev_config_file) do
      create_file(
        dev_config_file,
        dev_config_template(
          app_name: app_name,
          repository: Helpers.repo_name_to_module(processor_name, processor_repo),
          database:
            "#{Helpers.processor_type(processor_name) |> Macro.underscore()}_#{
              processor_name |> Helpers.short_processor_name()
            }",
          repo_path:
            "priv/#{Helpers.processor_type(processor_name) |> Macro.underscore()}/#{
              processor_repo |> Macro.underscore()
            }"
        )
      )
    end

    [processor_config_directory, processor_config_file]
  end

  embed_template(
    :processor_config,
    from_file: Path.join([__ENV__.file |> Path.dirname(), "templates", "processor_config.eex"])
  )

  embed_template(
    :dev_config,
    """
    use Mix.Config

    config :<%= @app_name%>, <%= @repository %>,
    database: "<%= @database %>",
    hostname: "localhost",
    show_sensitive_data_on_connection_error: true,
    pool_size: 10,
    priv: "<%= @repo_path %>"
    """
  )
end
