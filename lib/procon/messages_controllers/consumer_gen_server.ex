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
      "🦑❎ PROCON: #{__MODULE__} : starting #{initial_state.consumer_process_register_name}",
      __MODULE__
    )

    create_ets_table(initial_state)

    start_consumer_for_topic(initial_state)
    |> case do
      {:ok, pid} ->
        new_state = Map.put_new(initial_state, :link_group_subscriber_pid, pid)

        Procon.Helpers.olog(
          "🦑❎❎❎ PROCON: #{__MODULE__} : #{initial_state.consumer_process_register_name} started!",
          __MODULE__
        )

        {:ok, new_state}

      {:error, error} ->
        Procon.Helpers.olog(
          "🦑❌❌❌ PROCON: #{__MODULE__} : #{initial_state.consumer_process_register_name} not started!",
          __MODULE__
        )

        {:stop, error}
    end
  end

  @spec do_terminate(atom | pid | {atom, any} | {:via, atom, any}) :: :ok
  def do_terminate(pid_or_name) do
    GenServer.cast(pid_or_name, :do_terminate)
  end

  def delete_ets_table(ets_table_name) do
    :ets.delete(ets_table_name)

    Procon.Helpers.olog(
      "🦑❎ PROCON: #{__MODULE__} : deleted ets table #{ets_table_name}",
      __MODULE__
    )
  end

  @spec create_ets_table(atom | %{:ets_table_name => atom, optional(any) => any}) :: any
  def create_ets_table(state) do
    :ets.whereis(state.ets_table_name)
    |> case do
      :undefined ->
        :ets.new(state.ets_table_name, [:set, :public, :named_table])

        Procon.Helpers.olog(
          "🦑❎ PROCON: #{__MODULE__} : created ets table #{state.ets_table_name}",
          __MODULE__
        )

      _ ->
        Procon.Helpers.olog(
          "🦑⚠️ PROCON: #{__MODULE__} : ets table #{state.ets_table_name} already exists, creation not executed",
          __MODULE__
        )
    end
  end

  def handle_cast(:do_terminate, _from, state) do
    Procon.Helpers.olog(
      "🦑❎ PROCON: #{__MODULE__} : consumer #{state.register_name} terminating (do_terminate cast)...",
      __MODULE__
    )

    :ok = :brod_group_subscriber_v2.stop(state.link_group_subscriber_pid)
    delete_ets_table(state.ets_table_name)

    Procon.Helpers.olog(
      "🦑❎ PROCON: #{__MODULE__} : consumer #{state.register_name} terminated (do_terminate cast)!",
      __MODULE__
    )

    {:stop, :normal, :ok, nil}
  end

  def handle_info(msg, state) do
    IO.inspect(msg, label: "🦑❎❌ PROCON: handle_info msg #{state.consumer_process_register_name}")
    {:noreply, state}
  end

  def terminate(reason, state) do
    IO.inspect(state,
      label:
        "🦑❎❌ PROCON: #{state.consumer_process_register_name} terminating\n#{Kernel.inspect(reason)}"
    )

    :ok = :brod_group_subscriber_v2.stop(state.link_group_subscriber_pid)
    delete_ets_table(state.ets_table_name)

    :normal
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
          "🦑❎ PROCON: #{__MODULE__}.start_consumer_for_topic: registered group_subscriber #{state.brod_kafka_group_id}. pid : #{Kernel.inspect(pid)}",
          __MODULE__
        )

        {:ok, pid}

      {:error, error} ->
        Procon.Helpers.olog(
          "🦑❌ PROCON: #{__MODULE__}.start_consumer_for_topic: unable to start a consumer #{state.brod_kafka_group_id} > #{Kernel.inspect(error)}",
          __MODULE__
        )

        {:error, error}
    end
  end
end
