defmodule Procon.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    Procon.KafkaMetadata.cache_kafka_metadata()
    # List all child processes to be supervised
    children = [
      # Starts a worker by calling: Procon.Worker.start_link(arg)
      # {Procon.Worker, arg},
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Procon.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def start_erl_kaf() do
    producer_config = [
      {:bootstrap_servers, "localhost:9092"},
      {:delivery_report_only_error, false},
      {:delivery_report_callback, &delivery_report/2}
    ]

    :erlkaf.create_producer(:client_producer, producer_config)
    :erlkaf.produce(:client_producer, "refresh_events", "azer", "coucou")
  end

  def delivery_report(delivery_status, message) do
    IO.inspect delivery_status
    IO.inspect message
    :ok
  end

end
