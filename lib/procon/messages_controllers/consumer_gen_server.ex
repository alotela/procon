defmodule Procon.MessagesControllers.ConsumerGenServer do
  use GenServer

  def start_link(options) do
    GenServer.start_link(
      __MODULE__,
      options,
      name: options.consumer_process_register_name
    )
  end

  def init(initial_state) do
    Logger.metadata(
      procon_consumer_process_register_name: initial_state.consumer_process_register_name
    )

    Procon.Helpers.olog(
      "Procon.MessagesControllers.ConsumerGenServer : starting #{initial_state.consumer_process_register_name}",
      Procon.MessagesControllers.ConsumerGenServer
    )

    create_ets_table(initial_state)
    start_brod_client_for_consumers(initial_state, nil, nil)
    start_consumer_for_topic(initial_state)

    {:ok, initial_state}
  end

  def delete_ets_table(ets_table_name) do
    :ets.delete(ets_table_name)
    |> Procon.Helpers.olog(
      "Procon.MessagesControllers.ConsumerGenServer : deleted ets table #{ets_table_name}",
      Procon.MessagesControllers.ConsumerGenServer
    )
  end

  @spec create_ets_table(atom | %{:ets_table_name => atom, optional(any) => any}) :: any
  def create_ets_table(state) do
    :ets.whereis(state.ets_table_name)
    |> case do
      :undefined ->
        :ets.new(state.ets_table_name, [:set, :public, :named_table])
        |> Procon.Helpers.olog(
          "Procon.MessagesControllers.ConsumerGenServer : created ets table #{state.ets_table_name}",
          Procon.MessagesControllers.ConsumerGenServer
        )

      _ ->
        Procon.Helpers.olog(
          "Procon.MessagesControllers.ConsumerGenServer : ets table #{state.ets_table_name} already exists, creation not executed",
          Procon.MessagesControllers.ConsumerGenServer
        )
    end
  end

  # def terminate(reason, state) do
  #  IO.inspect(state, label: "genserver terminating... #{Kernel.inspect(reason)}")
  #
  #  stop_client(state.brod_client_name_for_consumers)
  #
  #  :normal
  # end

  def stop_client(client_name) do
    :brod.stop_client(client_name)
    |> Procon.Helpers.olog(
      "Procon.MessagesControllers.ConsumerGenServer : stop_client #{client_name}",
      Procon.MessagesControllers.ConsumerGenServer
    )
  end

  def stop_consumer(state) do
    stop_client(state.client_name)
    delete_ets_table(state.ets_table_name)
  end

  def start_brod_client_for_consumers(
        state,
        brokers,
        brod_client_config
      ) do
    :brod.start_client(
      brokers || Application.get_env(:procon, :brokers),
      state.brod_client_name_for_consumers,
      brod_client_config || Application.get_env(:procon, :brod_client_config)
    )
    |> case do
      :ok ->
        Procon.Helpers.olog(
          "❎❎❎❎❎❎❎❎❎❎❎❎ [client] Procon.MessagesControllers.ConsumerGenServer > start_brod_client_for_consumers: client started as #{state.brod_client_name_for_consumers} for #{state.consumer_process_register_name}.",
          Procon.MessagesControllers.ConsumerGenServer
        )

      error ->
        Procon.Helpers.olog(
          "❌❌❌❌❌❌❌❌❌❌❌❌[client] Procon.MessagesControllers.ConsumerGenServer > start_brod_client_for_consumers: unable to start a client for #{state.consumer_process_register_name} > #{Kernel.inspect(error)}",
          Procon.MessagesControllers.ConsumerGenServer
        )
    end
  end

  def start_consumer_for_topic(state) do
    :brod.start_link_group_subscriber_v2(%{
      client: state.brod_client_name_for_consumers,
      group_id: state.brod_kafka_group_id,
      topics: Enum.map(state.processor_consumer_config.entities, & &1.topic),
      group_config: [
        offset_commit_policy: :commit_to_kafka_v2,
        offset_commit_interval_seconds:
          Application.get_env(:procon, :offset_commit_interval_seconds)
      ],
      consumer_config: [
        begin_offset: :earliest
      ],
      cb_module: Procon.MessagesControllers.Consumer,
      init_data: state
    })
    |> case do
      {:ok, pid} ->
        Procon.Helpers.olog(
          "❎❎❎❎❎❎❎❎❎❎❎❎ [consumer] Procon.MessagesControllers.ConsumerGenServer > start_consumer_for_topic: registered group_subscriber #{state.consumer_process_register_name}. pid : #{Kernel.inspect(pid)}",
          Procon.MessagesControllers.ConsumerGenServer
        )

      {:error, error} ->
        Procon.Helpers.olog(
          "❌❌❌❌❌❌❌❌❌❌❌❌ [consumer] Procon.MessagesControllers.ConsumerGenServer > start_consumer_for_topic: unable to start a consumer#{state.consumer_process_register_name} > #{Kernel.inspect(error)}",
          Procon.MessagesControllers.ConsumerGenServer
        )
    end
  end
end
