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

  def start_partition_production(topic, partition, nb_messages \\ 1000) do
    producer_name = "#{topic}_#{partition}"
    DynamicSupervisor.start_child(
      Procon.MessagesProducers.ProducersSupervisor, 
      {
        __MODULE__,
        initial_state: %{nb_messages: nb_messages, topic: topic, partition: partition},
        producer_name: producer_name
      }
    )
    GenServer.cast(via(producer_name), {:start_partition_production})
  end

  def init(initial_state) do
    :ok = :brod.start_producer(:brod_client_1, initial_state.topic, [])
    new_initial_state = Map.merge(
      initial_state,
      %{
          brod_client: :brod_client_1,
          producing: false,
      }
    )
    {:ok, new_initial_state}
  end

  def handle_info(:produce, state) do
    case produce_next_messages(state) do
      {:no_more_messages} -> {:noreply, %{state | :producing => false}}
      {:ok, _message_index} ->
        send(self(), :produce)
        {:noreply, state}
      _error -> 
        {:noreply, %{state | :producing => false}}
    end
  end

  def start_next_production(state) do
    case Map.get(state, :producing) do
      true -> 
        :already_producing
      _ ->
        send(self(), :produce)
        :start_producing
    end
  end

  def handle_cast({:start_partition_production}, state) do
    case start_next_production(state) do
      :already_producing -> {:noreply, state}
      :start_producing -> {:noreply, %{state | :producing => true}}
    end  
  end

  def produce_next_messages(state) do
    case Procon.MessagesProducers.Ecto.next_messages_to_send(state.topic, state.partition, state.nb_messages) do
      [] -> {:no_more_messages}
      messages ->
          with :ok <- :brod.produce_sync(
            state.brod_client,
            state.topic,
            state.partition,
            "",
            Procon.Parallel.pmap(
              messages,
              fn(message) -> {"", String.replace(message.blob, "\"index\":1,\"event\":", "\"index\":#{message.id},\"event\":")} end)
          )
          {:ok, :next} <- Procon.MessagesProducers.Ecto.delete_rows(Enum.map(messages, &(&1.id)))
        do
          {:ok, messages |> Enum.reverse |> hd() |> Map.get(:id)}
        else
          {:error, error} -> {:error, error}
          _ -> {:error}
        end
    end
  end
end
