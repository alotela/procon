defmodule Procon.MessagesProducers.WalDispatcher do
  use GenServer
  alias Procon.MessagesProducers.EpgsqlConnector
  alias Procon.MessagesProducers.WalDispatcherProducer
  alias Procon.MessagesProducers.WalDispatcherMessagesQueueCleaner
  require Logger

  defmodule State do
    @enforce_keys [:datastore, :publications, :register_name]
    defstruct [
      :config,
      :datastore,
      :ets_messages_queue_ref,
      :processor_name,
      :producer_ets_state_table_name,
      :producer_ets_table_name,
      :publications,
      :realtime,
      :register_name,
      :replication_slot_name,
      brokers: [localhost: 9092],
      brod_client_config: [reconnect_cool_down_seconds: 10],
      broker_client_name: :brod_client_default_name,
      column_names_and_types: %{},
      delete_metadata: %{},
      epgsql_pid: nil,
      relations: %{},
      relation_configs: %{},
      slot_name: nil,
      wal_position: {"0", "0"},
      ets_table_state_ref: nil
    ]

    @type t() :: %__MODULE__{
            brokers: keyword({atom(), integer()}),
            broker_client_name: atom(),
            brod_client_config: keyword({atom(), any()}),
            column_names_and_types: map(),
            config: map(),
            datastore: atom(),
            delete_metadata: map(),
            epgsql_pid: pid() | nil,
            ets_messages_queue_ref: reference(),
            processor_name: atom(),
            producer_ets_state_table_name: atom(),
            producer_ets_table_name: atom(),
            publications: list(),
            realtime: boolean(),
            register_name: atom(),
            relation_configs: map(),
            replication_slot_name: String.t(),
            relations: map(),
            slot_name: String.t() | nil,
            wal_position: {String.t(), String.t()},
            ets_table_state_ref: reference()
          }
  end

  def start_link(%State{} = state) do
    GenServer.start_link(__MODULE__, state, name: state.register_name)
  end

  def do_terminate(pid_or_name) do
    GenServer.cast(pid_or_name, :do_terminate)
  end

  def add_delete_metadata(process_name, entity_primary_key, metadata),
    do: GenServer.call(process_name, {:add_delete_metadata, entity_primary_key, metadata})

  def init(%State{} = state) do
    Logger.metadata(procon_wal_dispatcher: state.datastore)

    Procon.Helpers.olog(
      "🦑❎ PROCON: #{__MODULE__} : Starting WalDispatcher for processor #{state.register_name}",
      __MODULE__
    )

    {:ok, producer_ets_table_name} = create_ets_table(state.producer_ets_table_name)

    producer_ets_state_table_name =
      :ets.new(state.producer_ets_state_table_name,
        write_concurrency: true,
        read_concurrency: true
      )

    start_realtime_producer(state)

    Process.flag(:trap_exit, true)

    Process.send(state.register_name, {:start_wal_stream}, [])

    {:ok,
     %State{
       state
       | ets_messages_queue_ref: producer_ets_table_name,
         ets_table_state_ref: producer_ets_state_table_name
     }}
  end

  defp relation_config_avro_value_schema(nil), do: ""

  defp relation_config_avro_value_schema(relation_config),
    do: Map.get(relation_config, :avro_value_schema, "#{relation_config.topic}-value")

  defp relation_config_avro_key_schema(nil), do: ""

  defp relation_config_avro_key_schema(relation_config),
    do: Map.get(relation_config, :avro_key_schema, "#{relation_config.topic}-key")

  def create_ets_table(register_name) do
    case :ets.whereis(register_name) do
      :undefined ->
        ^register_name =
          :ets.new(register_name, [
            :named_table,
            :public,
            :ordered_set,
            write_concurrency: true,
            read_concurrency: true
          ])

        {:ok, :ets.whereis(register_name)}

      reference ->
        Logger.warning("🦑⚠️ PROCON: #{__MODULE__} : #{register_name} : ets table already exists.")

        {:ok, reference}
    end
  end

  def handle_call({:add_delete_metadata, entity_primary_key, metadata}, _from, state) do
    {
      :reply,
      :ok,
      %State{
        state
        | delete_metadata: Map.put(state.delete_metadata, entity_primary_key, metadata)
      }
    }
  end

  def handle_call({:get_and_delete_metadata, entity_primary_key}, _from, state) do
    {
      :reply,
      Map.get(state.delete_metadata, entity_primary_key),
      %State{
        state
        | delete_metadata: Map.delete(state.delete_metadata, entity_primary_key)
      }
    }
  end

  def handle_call(:ets_state, _from, state) do
    {
      :reply,
      {
        state.ets_table_state_ref,
        :ets.tab2list(state.ets_table_state_ref),
        state.ets_messages_queue_ref,
        :ets.tab2list(state.ets_messages_queue_ref)
      },
      state
    }
  end

  def handle_info({:start_wal_stream}, %State{relation_configs: relation_configs} = state)
      when relation_configs == %{},
      do: {:noreply, state}

  def handle_info({:start_wal_stream}, %State{} = state) do
    config = state.datastore.config()
    sleep_time = Enum.random(1..100)

    IO.inspect(
      label:
        "start_wal_stream for database  #{config |> Keyword.get(:database)} with sleeptime = #{sleep_time}"
    )

    try do
      %Procon.MessagesProducers.EpgsqlConnector.Config{
        database: config |> Keyword.get(:database),
        host: config |> Keyword.get(:hostname) |> String.to_charlist(),
        password: config |> Keyword.get(:password),
        replication: "database",
        slot: state.datastore,
        username: config |> Keyword.get(:username)
      }
      |> EpgsqlConnector.connect(state.relation_configs |> Map.keys())
      |> IO.inspect(
        label: "EpgsqlConnector.connect for database #{config |> Keyword.get(:database)}"
      )
      |> case do
        {:ok, %{epgsql_pid: epgsql_pid, replication_slot_name: slot_name}} ->
          Process.monitor(epgsql_pid)

          start_queue_cleaner(epgsql_pid, state.ets_messages_queue_ref, state.register_name)

          start_replication(
            state.wal_position,
            epgsql_pid,
            slot_name,
            state.publications
          )

          {:noreply,
           %State{
             state
             | epgsql_pid: epgsql_pid,
               replication_slot_name: slot_name
           }}

        {:error, :econnrefused} ->
          Logger.error(
            "Procon.MessagesProducers.WalDispatcher.handle_info : EpgsqlConnector.connect : error : :econnrefused -> retrying"
          )

          Process.send_after(self(), {:start_wal_stream}, 1000)

          {:noreply, state}

        {:error, reason} ->
          Logger.error(
            "Procon.MessagesProducers.WalDispatcher.handle_info : EpgsqlConnector.connect : error : #{reason}"
          )

          Process.send_after(self(), {:start_wal_stream}, 1000)

          {:noreply, state}

        error ->
          Logger.error(
            "Procon.MessagesProducers.WalDispatcher.handle_info : EpgsqlConnector.connect : error : #{inspect(error)}"
          )

          Process.send_after(self(), {:start_wal_stream}, 1000)

          {:noreply, state}
      end
    rescue
      e ->
        IO.inspect(e, label: "exception catchée....")
    end
  end

  def handle_info({:epgsql, _pid, {:x_log_data, _start_lsn, end_lsn, binary_msg}}, state) do
    Procon.MessagesProducers.PgWalDeserializer.process_wal_binary(
      binary_msg,
      %{
        column_names_and_types: state.column_names_and_types,
        ets_table_state_ref: state.ets_table_state_ref,
        ets_messages_queue_ref: state.ets_messages_queue_ref,
        end_lsn: end_lsn,
        metadata: state.delete_metadata,
        relation_configs: state.relation_configs
      }
    )
    # |> IO.inspect(label: "epgsql log data")
    |> case do
      {:ok, :relation,
       %{relation_id: relation_id, name: _name, column_names_and_types: column_names_and_types}} ->
        GenServer.cast(self(), :start_broker_producers)

        {:noreply,
         %State{
           state
           | column_names_and_types:
               Map.put(state.column_names_and_types, relation_id, column_names_and_types)
         }}

      {:ok, _operation, _data} ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    case pid == state.epgsql_pid do
      true ->
        Procon.Helpers.olog(
          "🦑⚠️ PROCON: epgsql process for processor #{state.processor_name} down. restarting...",
          __MODULE__
        )

        Process.send_after(self(), {:start_wal_stream}, 1000)

      false ->
        Procon.Helpers.inspect(reason, label: "false")
    end

    {:noreply, %{state | epgsql_pid: nil}}
  end

  def handle_info(msg, state) do
    IO.inspect(msg, label: "🦑❎❌ PROCON: handle_info msg #{state.register_name}")
    {:noreply, state}
  end

  def handle_cast(:start_broker_producers, state) do
    start_producers_for_all_relation_configs(state)
    {:noreply, state}
  end

  def start_replication(wal_position, epgsql_pid, slot_name, publications) do
    {xlog, offset} = wal_position

    EpgsqlConnector.start_replication(
      epgsql_pid,
      slot_name,
      xlog,
      offset,
      Atom.to_string(publications)
    )
  end

  def start_producers_for_all_relation_configs(state) do
    Enum.map(
      state.relation_configs,
      fn {relation, %{topic: topic_atom}} ->
        :ok = start_topic_production(state.broker_client_name, topic_atom)

        start_topic_wal_producers(relation, topic_atom, state)
      end
    )
  end

  def start_realtime_producer(state) do
    case state.realtime do
      true ->
        :ok =
          start_topic_production(
            state.broker_client_name,
            Procon.MessagesProducers.Realtime.realtime_topic()
          )

      false ->
        nil
    end
  end

  @spec start_topic_production(any, any) :: :error | :ok | {:error, :unkown_topic_in_broker}
  def start_topic_production(broker_client_name, topic) do
    :brod.start_producer(broker_client_name, topic, [])
    |> case do
      :ok ->
        :ok

      {:error, {{:badmatch, {:error, :unknown_topic_or_partition}}, _}} = error ->
        Logger.warning(
          "unable to start wal dispatcher for #{broker_client_name} : topic #{topic} does not exist."
        )

        error
    end
  end

  def start_topic_wal_producers(relation, topic, state) do
    nb_partitions = Procon.KafkaMetadata.nb_partitions_for_topic!(topic)

    relation_configs = Map.get(state, :relation_configs)

    Enum.map(
      0..(nb_partitions - 1),
      fn partition_index ->
        register_name =
          WalDispatcherProducer.register_name(state.datastore, topic, partition_index)

        DynamicSupervisor.start_child(
          Procon.MessagesProducers.ProducersSupervisor,
          %{
            start: {
              WalDispatcherProducer,
              :start_link,
              [
                %{
                  avro_value_schema_name:
                    relation_configs
                    |> Map.get(relation)
                    |> relation_config_avro_value_schema(),
                  avro_key_schema_name:
                    relation_configs
                    |> Map.get(relation)
                    |> relation_config_avro_key_schema(),
                  broker_client_name: state.broker_client_name,
                  ets_key: :"#{topic}_#{partition_index}",
                  ets_messages_queue_ref: state.ets_messages_queue_ref,
                  materialize_json_attributes:
                    relation_configs
                    |> Map.get(relation)
                    |> Map.get(:materialize_json_attributes, []),
                  name: register_name,
                  partition_index: partition_index,
                  pkey_column:
                    relation_configs
                    |> Map.get(relation)
                    |> Map.get(:pkey),
                  serialization:
                    state.relation_configs
                    |> Map.get(relation, %{})
                    |> Map.get(:serialization, :json),
                  topic: topic
                }
              ]
            },
            id: register_name
          }
        )
      end
    )
  end

  def start_queue_cleaner(epgsql_pid, ets_messages_queue_ref, register_name) do
    {:ok, _child_pid} =
      DynamicSupervisor.start_child(
        Procon.MessagesProducers.ProducersSupervisor,
        %{
          start:
            {WalDispatcherMessagesQueueCleaner, :start_link,
             [
               %WalDispatcherMessagesQueueCleaner.State{
                 epgsql_pid: epgsql_pid,
                 ets_messages_queue_ref: ets_messages_queue_ref,
                 register_name: :"#{register_name}_queue_cleaner",
                 run: true
               }
             ]},
          id: :"#{register_name}_queue_cleaner"
        }
      )
  end

  def handle_cast(:do_terminate, _from, state) do
    Procon.Helpers.olog(
      "🦑❎ PROCON: #{__MODULE__} : producer #{state.register_name} terminating (do_terminate cast)...",
      __MODULE__
    )

    delete_ets_table(state.ets_table_identifier)
    delete_ets_table(state.ets_messages_queue_ref)

    Procon.Helpers.olog(
      "🦑❎ PROCON: #{__MODULE__} : producer #{state.register_name} terminated (do_terminate cast)!",
      __MODULE__
    )

    {:stop, :normal, :ok, nil}
  end

  def terminate(reason, state) do
    IO.inspect(state,
      label: "🦑❎❌ PROCON: #{state.register_name} terminating\n#{Kernel.inspect(reason)}"
    )

    delete_ets_table(state.ets_table_identifier)
    delete_ets_table(state.ets_messages_queue_ref)

    :normal
  end

  def delete_ets_table(ets_table_name) do
    :ets.delete(ets_table_name)

    Procon.Helpers.olog(
      "🦑❎ PROCON: #{__MODULE__} : deleted ets table #{ets_table_name}",
      __MODULE__
    )
  end
end
