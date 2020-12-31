defmodule Procon.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    Procon.KafkaMetadata.cache_kafka_metadata()

    children = [
      # {Registry, keys: :unique, name: Procon.ProducersRegistry},
      {DynamicSupervisor,
       name: Procon.MessagesProducers.ProducersSupervisor, strategy: :one_for_one},
      # Procon.MessagesControllers.ConsumersStarter,
      Procon.MessagesProducers.WalDispatcherSupervisor
      # Starts a worker by calling: Procon.Worker.start_link(arg)
      # {Procon.Worker, arg},
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Procon.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def after_start() do
    :procon_enqueuers_thresholds =
      :ets.new(:procon_enqueuers_thresholds, [:named_table, :public, :set])

    # Procon.MessagesProducers.ProducersStarter.start_topics_production_from_database_messages()
    # Procon.MessagesControllers.ConsumersStarter.start_activated_processors()
    Procon.MessagesProducers.WalDispatcherSupervisor.start_activated_processors_producers()
  end
end
