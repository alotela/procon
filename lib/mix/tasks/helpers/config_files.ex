defmodule Mix.Tasks.Procon.Helpers.ConfigFiles do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def generate_config_files(app_name, processor_name, processor_repo) do
    processors_config_directory = Path.join(["config", "processors"])

    unless File.exists?(processors_config_directory) do
      Helpers.info("creating processors config directory #{processors_config_directory}")

      create_directory(processors_config_directory)
    end

    processor_config_directory =
      Path.join([processors_config_directory, Helpers.short_processor_name(processor_name)])

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
          database: processor_name |> Helpers.short_processor_name()
        )
      )
    end

    [processor_config_directory, processor_config_file]
  end

  embed_template(
    :processor_config,
    """
    use Mix.Config

    config :procon, Processors,
      "Elixir.<%= @processor_name %>": [
        deps: [
          # add your deps here, they will be merged with mix.exs deps
        ],
        consumers: [
        # %{
        #    datastore: <%= @repository %>,
        #    name: <%= @processor_name %>,
        #    entities: [
        #      %{
        #        bypass_message_index: true, # optional
        #        event_version: 1,
        #        keys_mapping: %{"key_from_event" => :key_in_your_schema}, # optional
        #        master_key: {:processor_schema_key, "key_from_event"}, # optional
        #        messages_controller: MessageControllerToHandleMessage, # optional
        #        model: YourEctoSchemaModule,
        #        topic: "the_topic_to_listen"
        #      }
        #    ]
        #  }
        ]
      ]

    if [__DIR__, "\#{Mix.env}.exs"] |> Path.join() |> File.exists?(), do: import_config("\#{Mix.env()}.exs")
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
end
