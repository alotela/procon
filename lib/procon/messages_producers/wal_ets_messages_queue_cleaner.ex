defmodule Procon.MessagesProducers.WalDispatcherMessagesQueueCleaner do
  use GenServer
  alias Procon.MessagesProducers.EpgsqlConnector
  require Logger

  defmodule State do
    @enforce_keys [:epgsql_pid, :ets_messages_queue_ref, :register_name, :run]
    defstruct [
      :epgsql_pid,
      :ets_messages_queue_ref,
      :register_name,
      run: false
    ]

    @type t() :: %__MODULE__{
            epgsql_pid: pid(),
            ets_messages_queue_ref: reference(),
            register_name: atom(),
            run: boolean()
          }
  end

  def start_link(%State{} = state) do
    GenServer.start_link(__MODULE__, state, name: state.register_name)
  end

  @spec init(Procon.MessagesProducers.WalDispatcherMessagesQueueCleaner.State.t()) ::
          {:ok, Procon.MessagesProducers.WalDispatcherMessagesQueueCleaner.State.t()}
  def init(%State{} = state) do
    GenServer.cast(self(), :start)
    {:ok, state}
  end

  def handle_cast(:start, state) do
    start(state)
    {:noreply, %State{state | run: true}}
  end

  def handle_cast(:stop, state) do
    {:noreply, %State{state | run: false}}
  end

  @spec start(Procon.MessagesProducers.WalDispatcherMessagesQueueCleaner.State.t()) :: nil
  def start(state),
    do:
      :ets.first(state.ets_messages_queue_ref)
      |> next(state)

  def next(_lsn_as_key, %State{run: false}), do: nil

  def next(:"$end_of_table", state) do
    Process.sleep(1)
    start(state)
  end

  def next(lsn_as_key, state) do
    :ets.match(
      state.ets_messages_queue_ref,
      {lsn_as_key, :"$1", :"$3", :"$2"}
    )
    |> case do
      [[:transaction_commit, true, _data]] ->
        EpgsqlConnector.acknowledge_lsn(state.epgsql_pid, lsn_as_key)

        :ets.delete(state.ets_messages_queue_ref, lsn_as_key)

        :ets.next(state.ets_messages_queue_ref, lsn_as_key)
        |> next(state)

      [[_, true, _data]] ->
        :ets.delete(state.ets_messages_queue_ref, lsn_as_key)

        :ets.next(state.ets_messages_queue_ref, lsn_as_key)
        |> next(state)

      [[_, false, _data]] ->
        start(state)
    end
  end
end
