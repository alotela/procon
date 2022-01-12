defmodule Procon.MessagesControllers.ConsumersDynamicSupervisor do
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
        max_restarts: 3,
        # default
        max_seconds: 5,
        # only one_for_one allowed
        strategy: :one_for_one
      )

    {:ok, flags}
  end

  def consumers_brod_client_name(processor_consumer_config_name),
    do:
      Application.get_env(:procon, :brod_client_name_for_consumers) ||
        Module.concat(Procon.BrodKafkaClient, processor_consumer_config_name)

  def start_activated_processor_consumers() do
    Procon.ProcessorConfigAccessor.activated_consumers_configs()
    |> Enum.map(&start_activated_processor_consumer/1)
  end

  @spec register_name(atom()) :: atom()
  def register_name(processor_consumer_config_name),
    do: Module.concat(Procon.GenServer.Consumers, processor_consumer_config_name)

  def ets_table_name(processor_consumer_config_name),
    do: Module.concat(Procon.Ets.Consumers, processor_consumer_config_name)

  def brod_kafka_group_id(processor_consumer_config_name),
    do: "Procon.ConsumerGenServer.KafkaGroupId.#{processor_consumer_config_name}"

  def start_activated_processor_consumer(processor_consumer_config) do
    register_name = register_name(processor_consumer_config.name)

    DynamicSupervisor.start_child(
      Procon.MessagesControllers.ConsumersDynamicSupervisor,
      %{
        id: Module.concat(register_name, ChildId),
        restart: :temporary,
        start: {
          Procon.MessagesControllers.ConsumerGenServer,
          :start_link,
          [
            %{
              ets_table_name: ets_table_name(processor_consumer_config.name),
              brod_kafka_group_id: brod_kafka_group_id(processor_consumer_config.name),
              brod_client_name_for_consumers:
                consumers_brod_client_name(processor_consumer_config.name),
              consumer_process_register_name: register_name,
              processor_consumer_config: processor_consumer_config
            }
          ]
        }
      }
    )
    |> case do
      {:ok, child_pid} ->
        {:ok, child_pid}

      {:error, :max_children} ->
        Procon.Helpers.olog(
          "start_activated_processor_consumer ets table for processor consumer named #{processor_consumer_config.name}",
          Procon.MessagesControllers.ConsumersDynamicSupervisor
        )

      :ignore ->
        Procon.Helpers.olog(
          "start_activated_processor_consumer processor consumer named #{processor_consumer_config.name} not started (:ignore)",
          Procon.MessagesControllers.ConsumersDynamicSupervisor
        )

        :ignore

      error ->
        Procon.Helpers.olog(
          "start_activated_processor_consumer processor consumer named #{processor_consumer_config.name} not started\n#{Kernel.inspect(error)})",
          Procon.MessagesControllers.ConsumersDynamicSupervisor
        )

        error
    end
  end
end
