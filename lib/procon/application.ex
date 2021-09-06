defmodule Procon.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  require Logger

  use Application

  def start(_type, _args) do
    init_before_app_started()

    children =
      case Application.get_env(:procon, :autostart, true) do
        true ->
          Procon.KafkaMetadata.cache_kafka_metadata()

          [
            Avrora,
            {DynamicSupervisor,
             name: Procon.MessagesProducers.ProducersSupervisor, strategy: :one_for_one},
            Procon.MessagesControllers.ConsumersStarter,
            Procon.MessagesProducers.WalDispatcherSupervisor
          ]

        _ ->
          []
      end

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Procon.Supervisor]

    Supervisor.start_link(children, opts)
    |> case do
      {:ok, pid} ->
        init_after_app_started()
        {:ok, pid}

      error ->
        error
    end
  end

  def init_before_app_started() do
    Application.put_env(
      :procon,
      :logs,
      System.get_env("PROCON_LOGS", "")
      |> String.split(",", trim: true)
      |> Enum.map(&String.to_atom/1)
    )
  end

  def init_after_app_started() do
  end

  def after_start() do
    Application.get_env(:procon, :autostart, true)
    |> case do
      false ->
        Logger.warning("🤔🤔🤔 procon autostart == FALSE 🤔🤔🤔")
        nil

      _ ->
        do_start()
    end
  end

  def do_start() do
    Logger.info("👁 👁 starting procon")

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
