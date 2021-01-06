defmodule Procon.MessagesProducers.PgWalDeserializer do
  use Bitwise

  # https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html

  def process_wal_binary(
        <<"B", txn_commit_lsn::binary-8, timestamp::integer-64, xid::integer-32>>,
        %{
          ets_table_state_ref: ets_table_state_ref
        }
      ) do
    true =
      :ets.insert(
        ets_table_state_ref,
        {:txn, xid, txn_commit_lsn, timestamp}
      )

    {:ok, :begin_transaction, %{xid: xid, txn_commit_lsn: txn_commit_lsn}}
  end

  def process_wal_binary(
        <<"C", _flags::binary-1, commit_lsn::binary-8, txn_end_lsn::binary-8,
          _timestamp::integer-64>>,
        %{
          ets_table_state_ref: ets_table_state_ref,
          ets_messages_queue_ref: ets_messages_queue_ref
        }
      ) do
    case commit_lsn == :ets.lookup_element(ets_table_state_ref, :txn, 3) do
      true ->
        <<decimal_lsn::integer-64>> = txn_end_lsn
        :ets.insert(ets_table_state_ref, {:txn, nil, nil, nil})

        # this is used to commit offset to PG
        # only "Commit message" lsn can update PG wal log index
        :ets.insert(
          ets_messages_queue_ref,
          {decimal_lsn, :transaction_commit, nil, true}
        )

        {:ok, :commit_transaction, %{commit_lsn: commit_lsn}}

      false ->
        {:error, :commit_transaction,
         %{
           error:
             "Procon > producer > process_wal_binary > Commit > commit_lsn ne match pas avec le commit de transaction"
         }}
    end
  end

  def process_wal_binary(<<"O", _lsn::binary-8, _name::binary>>, _), do: {:ok, :o, nil}

  def process_wal_binary(<<"R", relation_id::integer-32, rest::binary>>, %{
        ets_table_state_ref: ets_table_state_ref
      }) do
    [
      _namespace
      | [
          name
          | [<<_replica_identity::binary-1, _number_of_columns::integer-16, columns::binary>>]
        ]
    ] = String.split(rest, <<0>>, parts: 3)

    name_atom = String.to_atom(name)

    %{^name_atom => {partition_key_column_atom, topic_atom}} =
      :ets.lookup_element(ets_table_state_ref, :relation_topics, 2)

    column_names = decode_columns(columns)

    partition_key_index =
      Enum.find_index(column_names, fn column -> column == partition_key_column_atom end)

    true =
      :ets.insert(
        ets_table_state_ref,
        {relation_id, name_atom, column_names, partition_key_index, topic_atom}
      )

    {:ok, :relation, %{relation_id: relation_id, name: name, column_names: column_names}}
  end

  def process_wal_binary(
        <<"I", relation_id::integer-32, "N", number_of_columns::integer-16, tuple_data::binary>>,
        %{
          column_names: column_names,
          ets_table_state_ref: ets_table_state_ref,
          ets_messages_queue_ref: ets_messages_queue_ref,
          end_lsn: end_lsn
        }
      ) do
    {<<>>, column_values} = decode_tuple_data(tuple_data, number_of_columns)
    IO.inspect(column_values, label: "column values")
    partition_key_column_index = :ets.lookup_element(ets_table_state_ref, relation_id, 4)
    IO.inspect(partition_key_column_index, label: "partition_key_column_index")
    topic_atom = :ets.lookup_element(ets_table_state_ref, relation_id, 5)
    IO.inspect(topic_atom, label: "topic_atom #{relation_id}")

    target_partition =
      column_values
      |> Enum.at(partition_key_column_index)
      |> select_partition(Procon.KafkaMetadata.nb_partitions_for_topic!(topic_atom))

    [[xid, timestamp]] = :ets.match(ets_table_state_ref, {:txn, :"$1", :_, :"$2"})

    :ets.insert(
      ets_messages_queue_ref,
      {end_lsn, :"#{topic_atom}_#{target_partition}",
       %{
         new: Enum.zip(column_names, column_values) |> Enum.into(%{}),
         timestamp: timestamp,
         xid: xid,
         end_lsn: end_lsn
       }, false}
    )

    IO.inspect(:ets.tab2list(ets_messages_queue_ref))

    {:ok, :insert, %{relation_id: relation_id, xid: xid, column_values: column_values}}
  end

  # insert en upsert : normalement on n'utilise pas cette forme
  def process_wal_binary(
        <<"U", _relation_id::integer-32, "N", _number_of_columns::integer-16,
          _tuple_data::binary>>,
        _
      ),
      do: {:ok, :upsert, nil}

  def process_wal_binary(
        <<"U", relation_id::integer-32, key_or_old::binary-1, old_number_of_columns::integer-16,
          tuple_data::binary>>,
        %{
          column_names: column_names,
          ets_table_state_ref: ets_table_state_ref,
          ets_messages_queue_ref: ets_messages_queue_ref,
          end_lsn: end_lsn
        }
      )
      when key_or_old == "O" or key_or_old == "K" do
    {<<"N", new_number_of_columns::integer-16, new_tuple_binary::binary>>, old_column_values} =
      decode_tuple_data(tuple_data, old_number_of_columns)

    {<<>>, new_column_values} = decode_tuple_data(new_tuple_binary, new_number_of_columns)
    partition_key_column_index = :ets.lookup_element(ets_table_state_ref, relation_id, 4)
    topic_atom = :ets.lookup_element(ets_table_state_ref, relation_id, 5)

    target_partition =
      new_column_values
      |> Enum.at(partition_key_column_index)
      |> select_partition(Procon.KafkaMetadata.nb_partitions_for_topic!(topic_atom))

    [[xid, timestamp]] = :ets.match(ets_table_state_ref, {:txn, :"$1", :_, :"$2"})

    :ets.insert(
      ets_messages_queue_ref,
      {end_lsn, :"#{topic_atom}_#{target_partition}",
       %{
         end_lsn: end_lsn,
         new: Enum.zip(column_names, new_column_values) |> Enum.into(%{}),
         old: Enum.zip(column_names, old_column_values) |> Enum.into(%{}),
         timestamp: timestamp,
         xid: xid
       }, false}
    )

    {:ok, :update, %{relation_id: relation_id, xid: xid, new_column_values: new_column_values}}
  end

  def process_wal_binary(
        <<"D", relation_id::integer-32, key_or_old::binary-1, number_of_columns::integer-16,
          tuple_data::binary>>,
        %{
          column_names: column_names,
          ets_table_state_ref: ets_table_state_ref,
          ets_messages_queue_ref: ets_messages_queue_ref,
          end_lsn: end_lsn
        }
      )
      when key_or_old == "K" or key_or_old == "O" do
    {<<>>, old_column_values} = decode_tuple_data(tuple_data, number_of_columns)
    partition_key_column_index = :ets.lookup_element(ets_table_state_ref, relation_id, 4)
    topic_atom = :ets.lookup_element(ets_table_state_ref, relation_id, 5)

    target_partition =
      old_column_values
      |> Enum.at(partition_key_column_index)
      |> select_partition(Procon.KafkaMetadata.nb_partitions_for_topic!(topic_atom))

    [[xid, timestamp]] = :ets.match(ets_table_state_ref, {:txn, :"$1", :_, :"$2"})

    :ets.insert(
      ets_messages_queue_ref,
      {end_lsn, :"#{topic_atom}_#{target_partition}",
       %{
         end_lsn: end_lsn,
         old: Enum.zip(column_names, old_column_values) |> Enum.into(%{}),
         timestamp: timestamp,
         xid: xid
       }, false}
    )

    {:ok, :delete, %{relation_id: relation_id, xid: xid, old_column_values: old_column_values}}
  end

  def process_wal_binary(
        <<"T", _number_of_relations::integer-32, _options::integer-8, _column_ids::binary>>,
        _
      ),
      do: {:ok, :truncate, nil}

  def process_wal_binary(
        <<"Y", _data_type_id::integer-32, _namespace_and_name::binary>>,
        _
      ),
      do: {:ok, :type, nil}

  def data_tuple_to_map(_columns, nil), do: %{}

  def data_tuple_to_map(columns, tuple_data) do
    for {column, index} <- Enum.with_index(columns, 1),
        do: {column.name, :erlang.element(index, tuple_data)},
        into: %{}
  end

  defp decode_columns(binary, accumulator \\ [])
  defp decode_columns(<<>>, accumulator), do: Enum.reverse(accumulator)

  defp decode_columns(<<_flags::integer-8, rest::binary>>, accumulator) do
    [name | [<<_data_type_id::integer-32, _type_modifier::integer-32, columns::binary>>]] =
      String.split(rest, <<0>>, parts: 2)

    decode_columns(columns, [String.to_atom(name) | accumulator])
  end

  defp decode_tuple_data(binary, columns_remaining, accumulator \\ [])

  defp decode_tuple_data(remaining_binary, 0, accumulator) when is_binary(remaining_binary),
    do: {remaining_binary, accumulator |> Enum.reverse()}

  defp decode_tuple_data(<<"n", rest::binary>>, columns_remaining, accumulator),
    do: decode_tuple_data(rest, columns_remaining - 1, [nil | accumulator])

  defp decode_tuple_data(<<"u", rest::binary>>, columns_remaining, accumulator),
    do: decode_tuple_data(rest, columns_remaining - 1, [:unchanged_toast | accumulator])

  defp decode_tuple_data(
         <<"t", column_length::integer-32, rest::binary>>,
         columns_remaining,
         accumulator
       ) do
    <<value::binary-size(column_length), rest_data::binary>> = rest

    decode_tuple_data(rest_data, columns_remaining - 1, [value | accumulator])
  end

  @spec select_partition(binary, integer) :: integer
  def select_partition(partition_key, nb_partitions) when is_binary(partition_key) do
    to_charlist(partition_key)
    |> Enum.reduce(0, fn charcode, hash -> (hash <<< 5) - hash + charcode end)
    |> abs()
    |> rem(nb_partitions)
  end
end
