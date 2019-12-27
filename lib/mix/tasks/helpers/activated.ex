defmodule Mix.Tasks.Procon.Helpers.Activated do
  import Mix.Generator

  def generate_config_activated(processor_name) do
    file = Path.join(["config", "processors", "activated.exs"])

    unless File.exists?(file) do
      create_file(file, file_template(processor_name: processor_name))
    end
    [file]
  end

  embed_template(
    :file,
    """
    use Mix.Config

    config :procon,
      :activated_processors: [
        <%= @processor_name %>
      ]
    """
  )
end
