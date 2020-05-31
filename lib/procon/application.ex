defmodule Procon.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    Procon.KafkaMetadata.cache_kafka_metadata()
    # List all child processes to be supervised
    :brod.start_client(
      Application.get_env(:procon, :brokers),
      Application.get_env(:procon, :broker_client_name),
      Application.get_env(:procon, :brod_client_config)
    )

    children = [
      {Registry, keys: :unique, name: Procon.ProducersRegistry},
      {DynamicSupervisor,
       name: Procon.MessagesProducers.ProducersSupervisor, strategy: :one_for_one},
      Procon.MessagesControllers.ConsumersStarter
      # Starts a worker by calling: Procon.Worker.start_link(arg)
      # {Procon.Worker, arg},
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Procon.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def after_start() do
    Procon.MessagesProducers.ProducersStarter.start_topics_production_from_database_messages()
    Procon.MessagesControllers.ConsumersStarter.start_activated_processors()
  end
end
