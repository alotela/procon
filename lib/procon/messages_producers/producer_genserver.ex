defmodule Procon.MessagesProducers.ProducerGenServer do
  use GenServer
  alias Procon.MessagesProducers.ProducerLastIndex
  alias Procon.MessagesProducers.ProducerSequences

  def via(producer_name), do: {:via, Registry, {Procon.ProducersRegistry, producer_name}}

  def start_link(state) do
    GenServer.start_link(
      __MODULE__,
      state,
      name: state.producer_name
    )
  end

  def start_partition_production(partition, nb_messages, processor_repo, topic) do
    producer_name = :"#{processor_repo}_#{topic}_#{partition}"
    DynamicSupervisor.start_child(
      Procon.MessagesProducers.ProducersSupervisor,
      %{
        start:
          {__MODULE__, :start_link,
           [
             %{
                nb_messages: nb_messages,
                partition: partition,
                producer_name: producer_name,
                repo: processor_repo,
                topic: topic,
                topic_partition: :"#{topic}_#{partition}"
             }
           ]},
        id: producer_name
      }
    )

    GenServer.cast(producer_name, {:start_partition_production})
  end

  def start_producer(repo, topic, partition) do
    ProducerSequences.create_sequence(repo, topic, partition, true)
    |> case do
      :ok ->
        :ok

      :error ->
        IO.inspect(repo,
          label:
            "PROCON ALERT : start_producer : producer on topic #{topic} and partition #{
              to_string(partition)
            } : error creating sequence",
          syntax_colors: [
            atom: :red,
            binary: :red,
            boolean: :red,
            list: :red,
            map: :red,
            number: :red,
            regex: :red,
            string: :red,
            tuple: :red
          ]
        )

        :error
    end

    ProducerLastIndex.init_sequence(repo, topic, partition)
    |> case do
      :ok ->
        start_partition_production(
          partition,
          Application.get_env(:procon, :nb_simultaneous_messages_to_send),
          repo,
          topic
        )

      :error ->
        IO.inspect(repo,
          label:
            "PROCON ALERT : start_producer : producer on topic #{topic} and partition #{
              to_string(partition)
            } : error creating last index sequence",
          syntax_colors: [
            atom: :red,
            binary: :red,
            boolean: :red,
            list: :red,
            map: :red,
            number: :red,
            regex: :red,
            string: :red,
            tuple: :red
          ]
        )

        :error
    end
  end

  def init(initial_state) do
    :ok =
      :brod.start_producer(
        Application.get_env(:procon, :broker_client_name),
        initial_state.topic,
        []
      )

    new_initial_state =
      Map.merge(
        initial_state,
        %{
          brod_client: Application.get_env(:procon, :broker_client_name),
          messages_queue: :queue.new(),
          producing: false,
          recovering_error: false
        }
      )

    Logger.metadata(procon_processor_repo: new_initial_state.repo)
    {:ok, new_initial_state}
  end

  def handle_info(:produce, %{recovering_error: true} = state) do
    # We are currently recovering from an error, we will retry later
    IO.inspect(state,
      label:
        "PROCON ALERT : producer on topic #{state.topic} and partition #{
          to_string(state.partition)
        } : recovering from an error. Retrying production later",
      syntax_colors: [
        atom: :red,
        binary: :red,
        boolean: :red,
        list: :red,
        map: :red,
        number: :red,
        regex: :red,
        string: :red,
        tuple: :red
      ]
    )

    Process.send_after(self(), :produce, 100)
    {:noreply, state}
  end

  def handle_info(:produce, state) do
    case produce_next_messages(state) do
      {:no_more_messages} ->
        {:noreply, %{state | :producing => false}}

<<<<<<< HEAD
      {:ok, message_index} ->
        ProducerLastIndex.set_last_produced_index(
          state.repo,
          state.topic,
          state.partition,
          message_index
        )
        |> case do
          {:ok, _idx} ->
            send(self(), :produce)
            {:noreply, state}

          :error ->
            IO.inspect(message_index,
              label:
                "PROCON ALERT : producer on topic #{state.topic} and partition #{
                  to_string(state.partition)
                } : error setting last index",
              syntax_colors: [
                atom: :red,
                binary: :red,
                boolean: :red,
                list: :red,
                map: :red,
                number: :red,
                regex: :red,
                string: :red,
                tuple: :red
              ]
            )

            send(self(), {:retry_set_last_index, message_index})
            {:noreply, %{state | recovering_error: true}}
        end

      {:error, :missing_ids} ->
        Process.send_after(self(), :produce, 100)
        {:noreply, state}

      {:error, :last_index} ->
        Process.send_after(self(), :produce, 100)
=======
      :ok ->
        send(self(), :produce)
>>>>>>> remove indexes
        {:noreply, state}

      {:error, :deleting_ids, ids, message_index} ->
        send(self(), {:retry_delete_rows, ids, message_index})
        {:noreply, %{state | recovering_error: true}}

      _error ->
        {:noreply, %{state | :producing => false}}
    end
  end

  def handle_info({:retry_set_last_index, message_index}, state) do
    ProducerLastIndex.set_last_produced_index(
      state.repo,
      state.topic,
      state.partition,
      message_index
    )
    |> case do
      {:ok, _idx} ->
        send(self(), :produce)
        {:noreply, %{state | recovering_error: false}}

      :error ->
        IO.inspect(message_index,
          label:
            "PROCON ALERT : producer on topic #{state.topic} and partition #{
              to_string(state.partition)
            } : error setting last index",
          syntax_colors: [
            atom: :red,
            binary: :red,
            boolean: :red,
            list: :red,
            map: :red,
            number: :red,
            regex: :red,
            string: :red,
            tuple: :red
          ]
        )

        Process.send_after(self(), {:retry_set_last_index, message_index}, 100)
        {:noreply, %{state | recovering_error: true}}
    end
  end

  def handle_info({:retry_delete_rows, ids, message_index}, state) do
    Procon.MessagesProducers.Ecto.delete_rows(
      state.repo,
      state.topic,
      state.partition,
      ids
    )
    |> case do
      {:ok, :next} ->
        Process.send_after(self(), {:retry_set_last_index, message_index}, 100)
        {:noreply, %{state | recovering_error: true}}

      {:stop, err} ->
        IO.inspect(err,
          label:
            "PROCON ALERT : producer on topic #{state.topic} and partition #{
              to_string(state.partition)
            } : error deleting ids",
          syntax_colors: [
            atom: :red,
            binary: :red,
            boolean: :red,
            list: :red,
            map: :red,
            number: :red,
            regex: :red,
            string: :red,
            tuple: :red
          ]
        )

        Process.send_after(self(), {:retry_delete_rows, ids, message_index}, 100)
        {:noreply, %{state | recovering_error: true}}
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
    case Procon.MessagesProducers.Ecto.next_messages_to_send(
           state.topic_partition,
           state.nb_messages,
           state.repo
         ) do
      [] ->
<<<<<<< HEAD
        {:no_more_messages}

      msg ->
        {[first_id | _rest] = ids, messages} = Enum.unzip(msg)
        last_id = ids |> Enum.reverse() |> hd()

        ProducerLastIndex.get_last_produced_index(state.repo, state.topic, state.partition)
        |> case do
          :error ->
            IO.inspect(state,
              label:
                "PROCON ALERT : producer on topic #{state.topic} and partition #{
                  to_string(state.partition)
                } : Getting last production index failed. Retrying production later",
              syntax_colors: [
                atom: :red,
                binary: :red,
                boolean: :red,
                list: :red,
                map: :red,
                number: :red,
                regex: :red,
                string: :red,
                tuple: :red
              ]
            )

            {:error, :last_index}

          last_produced_id ->
            do_produce_next_messages(
              first_id,
              last_id,
              last_produced_id,
              ids,
              messages,
              length(ids),
              state
            )
=======
        :no_more_messages

      id_blob_list ->
        {ids, blobs} = Enum.reduce(
          id_blob_list,
          {[], []},
          fn [id, blob], {ids, blobs} -> {[id|ids], [{"", blob}|blobs]} end
        )
        with :ok <-
               :brod.produce_sync(
                 state.brod_client,
                 state.topic,
                 state.partition,
                 "",
                 Enum.reverse(blobs)
               ),
             {:ok, :next} <-
               Procon.MessagesProducers.Ecto.delete_rows(state.repo, ids) do
          :ok
        else
          {:error, error} -> {:error, error}
          _ -> :error
>>>>>>> remove indexes
        end
    end
  end

  # The first message to produce is not the one absolutely just after the last produced one
  def do_produce_next_messages(
        first_id,
        _last_id,
        last_produced_id,
        ids,
        _messages,
        _messages_count,
        state
      )
      when first_id - 1 != last_produced_id do
    IO.inspect(ids,
      label:
        "PROCON ALERT : producer on topic #{state.topic} and partition #{
          to_string(state.partition)
        } : missing ids : last produced #{to_string(last_produced_id)}",
      syntax_colors: [
        atom: :red,
        binary: :red,
        boolean: :red,
        list: :red,
        map: :red,
        number: :red,
        regex: :red,
        string: :red,
        tuple: :red
      ]
    )

    {:error, :missing_ids}
  end

  # The sequence of ids received is not continuous
  def do_produce_next_messages(
        first_id,
        last_id,
        _last_produced_id,
        ids,
        _messages,
        messages_count,
        state
      )
      when last_id - first_id + 1 != messages_count do
    IO.inspect(ids,
      label:
        "PROCON ALERT : producer on topic #{state.topic} and partition #{
          to_string(state.partition)
        } : missing ids : not continuous sequence",
      syntax_colors: [
        atom: :red,
        binary: :red,
        boolean: :red,
        list: :red,
        map: :red,
        number: :red,
        regex: :red,
        string: :red,
        tuple: :red
      ]
    )

    {:error, :missing_ids}
  end

  def do_produce_next_messages(
        _first_id,
        last_id,
        _last_produced_id,
        ids,
        messages,
        _messages_count,
        state
      ) do
    with :ok <-
           :brod.produce_sync(
             state.brod_client,
             state.topic,
             state.partition,
             "",
             messages
           ),
         {:ok, :next} <-
           Procon.MessagesProducers.Ecto.delete_rows(
             state.repo,
             state.topic,
             state.partition,
             ids
           ) do
      {:ok, last_id}
    else
      {:stop, err} ->
        IO.inspect(err,
          label:
            "PROCON ALERT : producer on topic #{state.topic} and partition #{
              to_string(state.partition)
            } : error deleting ids",
          syntax_colors: [
            atom: :red,
            binary: :red,
            boolean: :red,
            list: :red,
            map: :red,
            number: :red,
            regex: :red,
            string: :red,
            tuple: :red
          ]
        )

        {:error, :deleting_ids, ids, last_id}

      {:error, error} ->
        IO.inspect(error,
          label:
            "PROCON ALERT : producer on topic #{state.topic} and partition #{
              to_string(state.partition)
            } : error in message production",
          syntax_colors: [
            atom: :red,
            binary: :red,
            boolean: :red,
            list: :red,
            map: :red,
            number: :red,
            regex: :red,
            string: :red,
            tuple: :red
          ]
        )

        {:error, error}

      err ->
        IO.inspect(err,
          label:
            "PROCON ALERT : producer on topic #{state.topic} and partition #{
              to_string(state.partition)
            } : error in message production",
          syntax_colors: [
            atom: :red,
            binary: :red,
            boolean: :red,
            list: :red,
            map: :red,
            number: :red,
            regex: :red,
            string: :red,
            tuple: :red
          ]
        )

        {:error, err}
    end
  end
end
