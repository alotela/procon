defmodule Procon.MessagesEnqueuers.EnqueuerGenServer do
  use GenServer
  alias Procon.Schemas.Ecto.ProconProducerMessage

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

    GenServer.call(enqueuer_name, {:enqueue, blob, processor_repo}, :infinity)
  end

  def init(initial_state) do
    {:ok, initial_state}
  end

  def handle_call({:enqueue, blob, processor_repo}, _from, state) do
    IO.inspect(processor_repo.in_transaction?, label: :in_transaction_in_handle_call?)
    do_enqueue(blob, state.partition, state.topic, processor_repo)
    |> case do
      {:ok, entity} ->
        {:reply, {:ok, entity}, state}

      {:error, err} ->
        {:reply, {:error, err}, state}
    end
  end

  def do_enqueue(blob, partition, topic, repo) do
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

    Ecto.Adapters.SQL.query(
      repo,
      "INSERT INTO procon_producer_messages (blob, partition, topic, inserted_at, updated_at)
       VALUES (
         replace(
           $1,
           '\"@@index@@\"',
           CAST (currval('procon_producer_messages_id_seq'::regclass) AS text)
         ),
         $2,
         $3,
         NOW(),
         NOW()
       ) RETURNING *",
      [blob, partition, topic]
    )
    |> case do
      {:ok, %Postgrex.Result{columns: columns, num_rows: 1, rows: [inserted_row]}} ->
        {:ok, repo.load(ProconProducerMessage, {columns, inserted_row})}

      {:error, %DBConnection.ConnectionError{} = err} ->
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
