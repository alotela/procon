defmodule Procon.MessagesProducers.ProducerGenServer do
  use GenServer

  def via(producer_name), do: {:via, Registry, {Procon.ProducersRegistry, producer_name}}

  def start_link(options) do
    GenServer.start_link(
      __MODULE__,
      Keyword.get(options, :initial_state),
      [
        name: via(Keyword.get(options, :producer_name))
      ]
    )
  end

  def start_production(producer_name) do
    GenServer.cast(via(producer_name), {:start_production})
  end

  def init(initial_state) do
    brod_producer_name = :"brod_producer_#{initial_state.topic}_#{initial_state.partition}"
    brod_producer = :brod.start_producer(brod_producer_name, initial_state.topic, [])
    new_initial_state = Map.merge(
      initial_state,
      %{
          brod_producer: brod_producer,
          brod_producer_name: brod_producer_name,
          producing: false,
          nb_messages: Map.get(initial_state, :nb_messages) || Application.get_env(:procon, :nb_simultaneous_messages_to_send)
      }
    )
    {:ok, new_initial_state}
  end

  def handle_cast({:start_production}, state) do
    case Map.get(state, :producing) do
      true -> nil 
      _ ->
        IO.inspect("handle cast start_production")
        GenServer.cast(self(), :produce_next_messages)
        IO.inspect("handle cast start_production > after process send")
        {:noreply, %{state | producing: true}}
    end
  end

  def produce_next_messages(state) do
    case Procon.MessagesProducers.Ecto.next_messages_to_send(state.topic, state.partition, state.nb_messages) do
      [] -> {:no_more_messages}
      messages ->
        with :ok <- :brod.produce_sync(state.brod_producer, state.topic, state.partition, "", messages),
             :ok <- (fn(messages) -> IO.inspect(messages) end).(messages),
             {} <- Procon.MessagesProducers.Ecto.delete_messages(Enum.map(messages, &(&1.id)))
        do
          {:ok, messages |> List.head |> elem(0)}
        else
          {:error, error} ->
            IO.inspect("error while producing messages. Stop")
            {:error, error}
          _ -> {:error}
        end
    end
  end

  def handle_cast({:produce_next_messages}, state) do
    IO.inspect(">2 handle_info produce_next_message #{state.producing} == true")
    case produce_next_messages(state) do
      {:no_more_messages} -> {:noreply, %{state | producing: false}}
      {:ok, message_index} -> 
        IO.inspect("handle_info pnm 2.1 #{message_index}")
        Process.send(self(), :produce_next_messages)
        IO.inspect("fin handle_info pnm 2.2 >1 #{message_index}")
        {:noreply, state}
      _ -> 
        IO.inspect("handle info pnm 3 _")
        {:noreply, state}
    end
  end
end
