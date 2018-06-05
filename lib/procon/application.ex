defmodule Procon.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    Procon.KafkaMetadata.cache_kafka_metadata()
    # List all child processes to be supervised
    :brod.start_client([localhost: 9092], :brod_client_1, [reconnect_cool_down_seconds: 10])
    
    children = [
      {Registry, keys: :unique, name: Procon.ProducersRegistry},
      {DynamicSupervisor, name: Procon.MessagesProducers.ProducersSupervisor, strategy: :one_for_one}
      # Starts a worker by calling: Procon.Worker.start_link(arg)
      # {Procon.Worker, arg},
    ]
    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Procon.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
