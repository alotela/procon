defmodule Procon.Processors.ProcessorsDynamicSupervisor do
  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    {:ok, flags} =
      DynamicSupervisor.init(
        # default extra_arguments added to all children state
        extra_arguments: [],
        # default
        max_children: :infinity,
        # default
        max_restarts: 10,
        # default
        max_seconds: 30,
        # only one_for_one allowed
        strategy: :one_for_one
      )

    {:ok, flags}
  end

  def start_activated_processors(processors_list \\ nil) do
    Procon.ProcessorConfigAccessor.activated_processors_configs(
      processors_list: processors_list,
      exclude_materialize_processors: true
    )
    |> Enum.map(&start_activated_processor/1)
  end

  def start_activated_processor({processor_name, processor_config}) do
    start_processor_brod_client(processor_name, processor_config)

    case Keyword.get(processor_config, :consumers, nil) do
      nil ->
        Procon.Helpers.olog(
          "🦑⚠️ PROCON: no kafka consumers started for processor_name #{processor_name}  (#{processor_brod_client_name(processor_name)}) ",
          __MODULE__
        )

        nil

      processor_consumer_configs when is_list(processor_consumer_configs) ->
        Enum.map(
          processor_consumer_configs,
          &start_activated_processor_consumer(processor_name, &1)
        )
    end

    case Keyword.get(processor_config, :producers, nil) do
      %{datastore: _datastore, procon_realtime: true} = producer_config ->
        start_activated_processor_producer(processor_name, producer_config)

      %{datastore: _datastore, relation_configs: relation_configs} when relation_configs == %{} ->
        nil

      %{datastore: _datastore, relation_configs: _relation_configs} = producer_config ->
        start_activated_processor_producer(processor_name, producer_config)

      nil ->
        nil
    end
  end

  def start_activated_processor_producer(processor_name, processor_producers_config) do
    register_name = producer_register_name(processor_name)

    DynamicSupervisor.start_child(
      Procon.MessagesProducers.ProducersSupervisor,
      %{
        start: {
          Procon.MessagesProducers.WalDispatcher,
          :start_link,
          [
            %Procon.MessagesProducers.WalDispatcher.State{
              brokers:
                Map.get(
                  processor_producers_config,
                  :brokers,
                  Application.get_env(:procon, :brokers)
                ),
              broker_client_name: processor_brod_client_name(processor_name),
              brod_client_config:
                Map.get(
                  processor_producers_config,
                  :brod_client_config,
                  Application.get_env(:procon, :brod_client_config)
                ),
              config: processor_producers_config,
              datastore: processor_producers_config.datastore,
              processor_name: processor_name,
              producer_ets_table_name: producer_ets_table_name(processor_name),
              producer_ets_state_table_name: producer_ets_state_table_name(processor_name),
              publications:
                Map.get(
                  processor_producers_config,
                  :publications,
                  :"procon_#{processor_producers_config.datastore.config() |> Keyword.get(:database)}"
                ),
              realtime: Map.get(processor_producers_config, :procon_realtime, false),
              register_name: register_name,
              relation_configs: Map.get(processor_producers_config, :relation_configs, %{})
            }
          ]
        },
        id: register_name
      }
    )
    |> case do
      {:ok, child_pid} ->
        Procon.Helpers.olog(
          "🦑❎ PROCON: kafka producer started for processor #{processor_name}. PID: #{Kernel.inspect(child_pid)}, NAME: #{register_name}",
          __MODULE__
        )

        {:ok, child_pid}

      {:error, :max_children} ->
        Procon.Helpers.olog(
          "🦑❌ PROCON: kafka producer not started for processor #{processor_name}. reason: {:error, :max_children} returned for Procon.Processors.ProcessorsDynamicSupervisor",
          __MODULE__
        )

      :ignore ->
        Procon.Helpers.olog(
          "🦑❌ PROCON: kafka producer #{register_name} not started for processor #{processor_name}. reason: :ignore returned for Procon.Processors.ProcessorsDynamicSupervisor",
          __MODULE__
        )

        :ignore

      error ->
        Procon.Helpers.olog(
          "🦑❌ PROCON: kafka producer #{register_name} not started for processor #{processor_name}. reason: \n#{Kernel.inspect(error)})",
          __MODULE__
        )

        error
    end
  end

  def start_processor_brod_client(processor_name, processor_config) do
    case Keyword.get(processor_config, :start_processor_client, true) do
      false ->
        Procon.Helpers.olog(
          "🦑⚠️ PROCON: no brod kafka client (#{processor_brod_client_name(processor_name)}) started for processor_name #{processor_name}",
          __MODULE__
        )

      _ ->
        :brod.start_client(
          Keyword.get(processor_config, :brokers, Application.get_env(:procon, :brokers)),
          processor_brod_client_name(processor_name),
          Keyword.get(
            processor_config,
            :brod_client_config,
            Application.get_env(:procon, :brod_client_config)
          )
        )
        |> case do
          :ok ->
            "🦑❎ PROCON: brod kafka client (#{processor_brod_client_name(processor_name)}) started for processor_name #{processor_name}"
            :ok

          error ->
            "🦑❌ PROCON: no brod kafka client (#{processor_brod_client_name(processor_name)}) started for processor_name #{processor_name}. Error: #{Kernel.inspect(error)}"
        end
    end
  end

  def is_processor_brod_kafka_client_started(processor_name) do
    Process.whereis(processor_brod_client_name(processor_name))
  end

  def processor_brod_client_name(processor_name),
    do: Module.concat(Procon.ProcessorBrodKafkaClient, processor_name)

  def consumer_register_name(processor_name),
    do: Module.concat(Procon.GenServer.Consumers, processor_name)

  def producer_register_name(processor_name),
    do: Module.concat(Procon.GenServer.Producers, processor_name)

  def consumer_ets_table_name(processor_name),
    do: Module.concat(Procon.Ets.Consumers, processor_name)

  def producer_ets_table_name(processor_name),
    do: Module.concat(Procon.Ets.Producers, processor_name)

  def producer_ets_state_table_name(processor_name),
    do: Module.concat(Procon.Ets.Producers.State, processor_name)

  def brod_kafka_group_id(processor_name),
    do: "Procon.ConsumerGenServers.KafkaGroupId.#{processor_name}"

  def start_activated_processor_consumer(processor_name, processor_consumer_config) do
    # Process.sleep(:rand.uniform(100))
    register_name = consumer_register_name(processor_name)

    DynamicSupervisor.start_child(
      Procon.Processors.ProcessorsDynamicSupervisor,
      %{
        id: Module.concat(register_name, ChildId),
        restart: :transient,
        start: {
          Procon.MessagesControllers.ConsumerGenServer,
          :start_link,
          [
            %{
              ets_table_name: consumer_ets_table_name(processor_consumer_config.datastore),
              brod_kafka_group_id: brod_kafka_group_id(processor_name),
              brod_client_name_for_consumers: processor_brod_client_name(processor_name),
              consumer_process_register_name: register_name,
              processor_consumer_config: processor_consumer_config
            }
          ]
        }
      }
    )
    |> case do
      {:ok, child_pid} ->
        Procon.Helpers.olog(
          "🦑❎ PROCON: kafka consumers started for processor #{processor_name}. PID: #{Kernel.inspect(child_pid)}, NAME: #{register_name}",
          __MODULE__
        )

        {:ok, child_pid}

      {:error, :max_children} ->
        Procon.Helpers.olog(
          "🦑❌ PROCON: kafka consumers not started for processor #{processor_name}. reason: {:error, :max_children} returned for Procon.Processors.ProcessorsDynamicSupervisor",
          __MODULE__
        )

      :ignore ->
        Procon.Helpers.olog(
          "🦑❌ PROCON: kafka consumers #{register_name} not started for processor #{processor_name}. reason: :ignore returned for Procon.Processors.ProcessorsDynamicSupervisor",
          __MODULE__
        )

        :ignore

      error ->
        Procon.Helpers.olog(
          "🦑❌ PROCON: kafka consumers #{register_name} not started for processor #{processor_name}. reason: #{Kernel.inspect(error)})",
          __MODULE__
        )

        error
    end
  end

  def stop_processor_brod_client(processor_name) do
    result = :brod.stop_client(processor_brod_client_name(processor_name))

    Procon.Helpers.olog(
      "🦑❎ PROCON: brod client stopped #{processor_brod_client_name(processor_name)}\n#{Kernel.inspect(result)}",
      __MODULE__
    )
  end

  def stop_consumer_child(processor_name) do
    processor_name
    |> consumer_register_name()
    |> Procon.MessagesControllers.ConsumerGenServer.do_terminate()
  end

  def stop_producer_child(processor_name) do
    processor_name
    |> producer_register_name()
    |> Procon.MessagesProducers.WalDispatcher.do_terminate()
  end
end
