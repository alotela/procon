defmodule Procon.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    Procon.KafkaMetadata.cache_kafka_metadata()

    children = [
      Avrora,
      {DynamicSupervisor,
       name: Procon.MessagesProducers.ProducersSupervisor, strategy: :one_for_one},
      Procon.MessagesControllers.ConsumersStarter,
      Procon.MessagesProducers.WalDispatcherSupervisor,
      Procon.Materialize.Starter
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Procon.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def after_start() do
    :procon_enqueuers_thresholds =
      :ets.new(:procon_enqueuers_thresholds, [:named_table, :public, :set])

    Procon.MessagesControllers.ConsumersStarter.start_activated_processors()
    Procon.MessagesProducers.WalDispatcherSupervisor.start_activated_processors_producers()

    # a virer apres migration
    :brod.start_client(
      Application.get_env(:procon, :brokers),
      :brod_client,
      Application.get_env(:procon, :brod_client_config)
    )
  end
end
