defmodule Procon.MessagesProducers.WalDispatcher do
  use GenServer
  alias Procon.MessagesProducers.EpgsqlConnector
  alias Procon.MessagesProducers.WalDispatcherProducer
  alias Procon.MessagesProducers.WalDispatcherMessagesQueueCleaner
  require Logger

  defmodule State do
    @enforce_keys [:datastore, :publications, :register_name, :relation_topics]
    defstruct [
      :datastore,
      :register_name,
      :ets_messages_queue_ref,
      :publications,
      :replication_slot_name,
      :relation_topics,
      brokers: [localhost: 9092],
      brod_client_config: [reconnect_cool_down_seconds: 10],
      broker_client_name: :brod_client_default_name,
      column_names: [],
      epgsql_pid: nil,
      relations: %{},
      slot_name: nil,
      wal_position: {"0", "0"},
      ets_table_state_ref: nil
    ]

    @type t() :: %__MODULE__{
            brokers: keyword({atom(), integer()}),
            broker_client_name: atom(),
            brod_client_config: keyword({atom(), any()}),
            column_names: list(),
            datastore: atom(),
            epgsql_pid: pid() | nil,
            ets_messages_queue_ref: reference(),
            publications: list(),
            register_name: atom(),
            relation_topics: map(),
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

  @spec register_name(atom()) :: atom()
  def register_name(datastore), do: :"wal_dispatcher_#{datastore}"

  def start_wal_dispatcher_for_processor(processor_producers_config) do
    register_name = register_name(processor_producers_config.datastore)

    {:ok, _child_pid} =
      DynamicSupervisor.start_child(
        Procon.MessagesProducers.ProducersSupervisor,
        %{
          start:
            {__MODULE__, :start_link,
             [
               %State{
                 brokers:
                   Map.get(
                     processor_producers_config,
                     :brokers,
                     Application.get_env(:procon, :brokers)
                   ),
                 broker_client_name:
                   Map.get(
                     processor_producers_config,
                     :broker_client_name,
                     :"#{register_name}_brod_client"
                   ),
                 brod_client_config:
                   Map.get(
                     processor_producers_config,
                     :brod_client_config,
                     Application.get_env(:procon, :brod_client_config)
                   ),
                 datastore: processor_producers_config.datastore,
                 publications:
                   Map.get(
                     processor_producers_config,
                     :publications,
                     :"procon_#{
                       processor_producers_config.datastore.config() |> Keyword.get(:database)
                     }"
                   ),
                 register_name: register_name,
                 relation_topics: processor_producers_config.relation_topics
               }
             ]},
          id: register_name
        }
      )
  end

  def init(%State{} = state) do
    Logger.metadata(procon_wal_dispatcher: state.datastore)

    Logger.notice("PROCON : Starting WalDispatcher for datastore #{state.datastore}")

    {:ok, ets_table_identifier} = create_ets_table(state.register_name)

    ets_table_state_ref = :ets.new(:ets_state, write_concurrency: true, read_concurrency: true)

    :ets.insert(
      ets_table_state_ref,
      {:relation_topics, state.relation_topics}
    )

    start_brod_client(state.brokers, state.broker_client_name, state.brod_client_config)

    Process.flag(:trap_exit, true)

    Process.send(state.register_name, {:start_wal_stream}, [])

    {:ok,
     %State{
       state
       | ets_messages_queue_ref: ets_table_identifier,
         ets_table_state_ref: ets_table_state_ref
     }}
  end

  def start_brod_client(brokers, broker_client_name, brod_client_config) do
    :ok = :brod.start_client(brokers, broker_client_name, brod_client_config)
  end

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
        Logger.warn(
          "Procon.MessagesProducers.WalDispatcher.create_ets_table : #{register_name} : ets table already exists."
        )

        {:ok, reference}
    end
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

  def handle_info({:start_wal_stream}, %State{} = state) do
    config = state.datastore.config()

    %Procon.MessagesProducers.EpgsqlConnector.Config{
      database: config |> Keyword.get(:database),
      host: config |> Keyword.get(:hostname) |> String.to_charlist(),
      password: config |> Keyword.get(:password),
      replication: "database",
      slot: state.datastore,
      username: config |> Keyword.get(:username)
    }
    |> EpgsqlConnector.connect(state.relation_topics |> Map.keys())
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

      {:error, reason} ->
        Logger.error(
          "Procon.MessagesProducers.WalDispatcher.handle_info : EpgsqlConnector.connect : error : #{
            reason
          }"
        )

        Process.send_after(self(), {:start_wal_stream}, 1000)

        {:noreply, state}

      error ->
        Logger.error(
          "Procon.MessagesProducers.WalDispatcher.handle_info : EpgsqlConnector.connect : error : #{
            inspect(error)
          }"
        )

        Process.send_after(self(), {:start_wal_stream}, 1000)

        {:noreply, state}
    end
  end

  def handle_info({:epgsql, _pid, {:x_log_data, _start_lsn, end_lsn, binary_msg}}, state) do
    Procon.MessagesProducers.PgWalDeserializer.process_wal_binary(
      binary_msg,
      %{
        column_names: state.column_names,
        ets_table_state_ref: state.ets_table_state_ref,
        ets_messages_queue_ref: state.ets_messages_queue_ref,
        end_lsn: end_lsn
      }
    )
    |> IO.inspect(label: "handle_info epgsql")
    |> case do
      {:ok, :relation, %{relation_id: _relation_id, name: _name, column_names: column_names}} ->
        GenServer.cast(self(), :start_broker_producers)
        {:noreply, %State{state | column_names: column_names}}

      {:ok, _operation, _data} ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    case pid == state.epgsql_pid do
      true ->
        Process.send_after(self(), {:start_wal_stream}, 1000)

      false ->
        Procon.Helpers.inspect(reason, label: "false")
    end

    {:noreply, %{state | epgsql_pid: nil}}
  end

  def handle_cast(:start_broker_producers, state) do
    start_producers_for_all_relation_topics(state)
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

  def start_producers_for_all_relation_topics(state) do
    Enum.map(
      state.relation_topics,
      fn {_relation, {_pkey_column, topic_atom}} ->
        start_topic_production(state.broker_client_name, topic_atom)

        start_topic_wal_producers(topic_atom, state)
      end
    )
  end

  @spec start_topic_production(any, any) :: :error | :ok | {:error, :unkown_topic_in_broker}
  def start_topic_production(broker_client_name, topic) do
    :ok = :brod.start_producer(broker_client_name, topic |> Atom.to_string(), [])
  end

  def start_topic_wal_producers(topic, state) do
    nb_partitions = Procon.KafkaMetadata.nb_partitions_for_topic!(topic)

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
                  broker_client_name: state.broker_client_name,
                  column_names: state.column_names,
                  ets_key: :"#{topic}_#{partition_index}",
                  ets_messages_queue_ref: state.ets_messages_queue_ref,
                  ets_table_state_ref: state.ets_table_state_ref,
                  partition_index: partition_index,
                  name: register_name,
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
end