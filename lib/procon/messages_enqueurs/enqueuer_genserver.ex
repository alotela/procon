defmodule Procon.MessagesEnqueuers.EnqueuerGenServer do
  use GenServer
  alias Procon.Schemas.Ecto.ProconProducerMessage
  alias Procon.MessagesProducers.ProducerSequences

  def via(enqueuer_name), do: {:via, Registry, {Procon.EnqueuersRegistry, enqueuer_name}}

  def start_link(state) do
    GenServer.start_link(
      __MODULE__,
      state,
      name: state.enqueuer_name
    )
  end

  def enqueue(blob, partition, topic, processor_repo) do
    enqueuer_name = :"enqueuer_#{processor_repo}_#{topic}_#{partition}"

    if is_nil(GenServer.whereis(enqueuer_name)) do
      DynamicSupervisor.start_child(
        Procon.MessagesEnqueuers.EnqueuersSupervisor,
        %{
          start:
            {__MODULE__, :start_link,
             [
               %{
                 partition: partition,
                 enqueuer_name: enqueuer_name,
                 repo: processor_repo,
                 topic: topic,
                 enqueued_blobs: []
               }
             ]},
          id: enqueuer_name
        }
      )
    end

    GenServer.call(enqueuer_name, {:enqueue, blob}, :infinity)
  end

  def init(initial_state) do
    {:ok, initial_state}
  end

  def handle_info(:retry_enqueue, %{enqueued_blobs: enqueued_blobs} = state) do
    blobs_to_retry =
      enqueued_blobs
      |> Enum.reverse()
      |> Enum.reduce([], fn {blob, from}, blobs ->
        do_enqueue(blob, state.partition, state.topic, state.repo, true)
        |> case do
          {:ok, entity} ->
            GenServer.reply(from, {:ok, entity})
            blobs

          {:error, _err} ->
            [{blob, from} | blobs]
        end
      end)

    if length(blobs_to_retry) > 0 do
      Process.send_after(self(), :retry_enqueue, 100)
    end

    {:noreply, %{state | enqueued_blobs: blobs_to_retry}}
  end

  def handle_call({:enqueue, blob}, from, %{enqueued_blobs: enqueued_blobs} = state)
      when length(enqueued_blobs) > 0 do
    IO.inspect(enqueued_blobs,
      label:
        "PROCON ALERT : new message enqueued on topic #{state.topic} and partition #{
          to_string(state.partition)
        } because queue was not empty",
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

    Process.send_after(self(), :retry_enqueue, 100)

    {:noreply, %{state | enqueued_blobs: [{blob, from} | enqueued_blobs]}}
  end

  def handle_call({:enqueue, blob}, from, state) do
    do_enqueue(blob, state.partition, state.topic, state.repo)
    |> case do
      {:ok, entity} ->
        {:reply, {:ok, entity}, state}

      {:error, _err} ->
        Process.send_after(self(), :retry_enqueue, 100)

        {:noreply, %{state | enqueued_blobs: [{blob, from} | state.enqueued_blobs]}}
    end
  end

  def do_enqueue(blob, partition, topic, repo, force \\ false) do
    #   {:ok,
    #  %Postgrex.Result{
    #    columns: ["id", "blob", "is_stopped", "partition", "stopped_error",
    #     "stopped_message_id", "topic", "inserted_at", "updated_at"],
    #    command: :insert,
    #    connection_id: 33389,
    #    messages: [],
    #    num_rows: 1,
    #    rows: [
    #      [7,
    #       "{\"body\":{\"1\":{\"account_id\":\"c411a06a-1b96-49de-b389-f17a95d20371\",\"id\":\"a73326e4-a06f-4d5e-9026-fa8dcc198886\",\"session_token\":\"73b2542b-3364-4353-91c3-dd934cd4beda\"}},\"event\":\"created\",\"index\":7}",
    #       nil, 0, nil, nil, "calions-int-evt-authentications",
    #       ~N[2020-09-30 11:09:49.000000], ~N[2020-09-30 11:09:49.000000]]
    #    ]
    #  }}

    ProducerSequences.create_sequence(repo, topic, partition, force)

    Ecto.Adapters.SQL.query(
      repo,
      "WITH curr_index
        AS (SELECT nextval('#{ProducerSequences.get_sequence_name(topic, partition)}'::regclass) AS idx)
        INSERT INTO procon_producer_messages (blob, index, partition, topic, inserted_at, updated_at)
      SELECT
        replace(
          $1,
          '\"@@index@@\"',
          idx::text
        ),
        idx,
        $2,
        $3,
        NOW(),
        NOW()
      FROM curr_index RETURNING *",
      [blob, partition, topic]
    )
    |> case do
      {:ok, %Postgrex.Result{columns: columns, num_rows: 1, rows: [inserted_row]}} ->
        {:ok, repo.load(ProconProducerMessage, {columns, inserted_row})}

      {:error, err} ->
        IO.inspect(err,
          label:
            "PROCON ALERT : enqueue error on topic #{topic} and partition #{to_string(partition)}",
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
