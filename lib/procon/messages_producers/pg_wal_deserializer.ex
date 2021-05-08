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
        ets_table_state_ref: ets_table_state_ref,
        relation_configs: relation_configs
      }) do
    [
      _namespace
      | [
          name
          | [<<_replica_identity::binary-1, _number_of_columns::integer-16, columns::binary>>]
        ]
    ] = String.split(rest, <<0>>, parts: 3)

    name_atom = String.to_atom(name)

    %{^name_atom => %{pkey: partition_key_column_atom, topic: topic_atom}} = relation_configs

    column_names_and_types = decode_columns(columns)

    partition_key_index =
      Enum.find_index(column_names_and_types, fn {column, _data_type_id} ->
        column == partition_key_column_atom
      end)

    true =
      :ets.insert(
        ets_table_state_ref,
        {relation_id, name_atom, column_names_and_types, partition_key_index, topic_atom}
      )

    {:ok, :relation,
     %{relation_id: relation_id, name: name, column_names_and_types: column_names_and_types}}
  end

  def process_wal_binary(
        <<"I", relation_id::integer-32, "N", number_of_columns::integer-16, tuple_data::binary>>,
        %{
          column_names_and_types: column_names_and_types,
          ets_table_state_ref: ets_table_state_ref,
          ets_messages_queue_ref: ets_messages_queue_ref,
          end_lsn: end_lsn
        }
      ) do
    {<<>>, column_values} = decode_tuple_data(tuple_data, number_of_columns)
    partition_key_column_index = :ets.lookup_element(ets_table_state_ref, relation_id, 4)
    topic_atom = :ets.lookup_element(ets_table_state_ref, relation_id, 5)

    target_partition =
      column_values
      |> Enum.at(partition_key_column_index)
      |> select_partition(Procon.KafkaMetadata.nb_partitions_for_topic!(topic_atom))

    [[xid, timestamp]] = :ets.match(ets_table_state_ref, {:txn, :"$1", :_, :"$2"})

    :ets.insert(
      ets_messages_queue_ref,
      {end_lsn, :"#{topic_atom}_#{target_partition}",
       %{
         payload: %{
           after:
             Enum.zip(Map.get(column_names_and_types, relation_id, []), column_values)
             |> Enum.reduce(%{}, &map_value_with_type/2),
           source: %{}
         },
         timestamp: timestamp,
         xid: xid,
         end_lsn: end_lsn
       }, false}
    )

    {:ok, :insert, %{relation_id: relation_id, xid: xid, column_values: column_values}}
  end

  # insert en upsert : normalement on n'utilise pas cette forme
  def process_wal_binary(
        <<"U", relation_id::integer-32, "N", number_of_columns::integer-16,
          tuple_binary::binary>>,
        %{
          column_names_and_types: column_names_and_types,
          ets_table_state_ref: ets_table_state_ref,
          ets_messages_queue_ref: ets_messages_queue_ref,
          end_lsn: end_lsn
        }
      ) do
    {<<>>, new_column_values} = decode_tuple_data(tuple_binary, number_of_columns)

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
         payload: %{
           after:
             Enum.zip(Map.get(column_names_and_types, relation_id, []), new_column_values)
             |> Enum.reduce(%{}, &map_value_with_type/2)
         },
         timestamp: timestamp,
         xid: xid
       }, false}
    )

    {:ok, :upsert,
     %{
       relation_id: relation_id,
       xid: xid,
       new_column_values: new_column_values,
       number_of_columns: number_of_columns
     }}
  end

  def process_wal_binary(
        <<"U", relation_id::integer-32, key_or_old::binary-1, old_number_of_columns::integer-16,
          tuple_data::binary>>,
        %{
          column_names_and_types: column_names_and_types,
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
         payload: %{
           after:
             Enum.zip(Map.get(column_names_and_types, relation_id, []), new_column_values)
             |> Enum.reduce(%{}, &map_value_with_type/2),
           before:
             Enum.zip(Map.get(column_names_and_types, relation_id, []), old_column_values)
             |> Enum.reduce(%{}, &map_value_with_type/2)
         },
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
          column_names_and_types: column_names_and_types,
          ets_table_state_ref: ets_table_state_ref,
          ets_messages_queue_ref: ets_messages_queue_ref,
          end_lsn: end_lsn,
          metadata: _delete_metadata
        }
      )
      when key_or_old == "K" or key_or_old == "O" do
    {<<>>, old_column_values} = decode_tuple_data(tuple_data, number_of_columns)
    partition_key_column_index = :ets.lookup_element(ets_table_state_ref, relation_id, 4)
    topic_atom = :ets.lookup_element(ets_table_state_ref, relation_id, 5)

    pkey_value = Enum.at(old_column_values, partition_key_column_index)

    target_partition =
      select_partition(pkey_value, Procon.KafkaMetadata.nb_partitions_for_topic!(topic_atom))

    [[xid, timestamp]] = :ets.match(ets_table_state_ref, {:txn, :"$1", :_, :"$2"})

    :ets.insert(
      ets_messages_queue_ref,
      {end_lsn, :"#{topic_atom}_#{target_partition}",
       %{
         end_lsn: end_lsn,
         payload: %{
           # add by hand metadata for http_request_id etc etc... since no record in new when deleting
           before:
             Enum.zip(Map.get(column_names_and_types, relation_id, []), old_column_values)
             |> Enum.reduce(%{}, &map_value_with_type/2)
         },
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
    [name | [<<data_type_id::integer-32, _type_modifier::integer-32, columns::binary>>]] =
      String.split(rest, <<0>>, parts: 2)

    decode_columns(columns, [{String.to_atom(name), data_type_id} | accumulator])
  end

  defp decode_tuple_data(binary, columns_remaining, accumulator \\ [])

  defp decode_tuple_data(<<remaining_binary::binary>>, 0, accumulator),
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

  # data type 3802 = jsonb (https://github.com/epgsql/epgsql/blob/devel/src/epgsql_binary.erl#L350)
  def map_value_with_type({{name, 3802}, value}, payload),
    do:
      Map.put(
        payload,
        name,
        case is_nil(value) do
          true ->
            %{}

          false ->
            Jason.decode!(value)
        end
      )

  def map_value_with_type({{name, 16}, value}, payload),
    do:
      Map.put(
        payload,
        name,
        case value do
          "t" ->
            true

          "f" ->
            false
        end
      )

  def map_value_with_type({{name, data_type_id}, value}, payload) do
    if not is_list_type(data_type_id) do
      Map.put(payload, name, value)
    else
      Map.put(payload, name, parse_pg_array!(value))
    end
  end

  def oids(),
    do: [
      {:bool, 16, 1000},
      {:bpchar, 1042, 1014},
      {:bytea, 17, 1001},
      {:char, 18, 1002},
      {:cidr, 650, 651},
      {:date, 1082, 1182},
      {:daterange, 3912, 3913},
      {:float4, 700, 1021},
      {:float8, 701, 1022},
      {:inet, 869, 1041},
      {:int2, 21, 1005},
      {:int4, 23, 1007},
      {:int4range, 3904, 3905},
      {:int8, 20, 1016},
      {:int8range, 3926, 3927},
      {:interval, 1186, 1187},
      {:json, 114, 199},
      {:jsonb, 3802, 3807},
      {:macaddr, 829, 1040},
      {:macaddr8, 774, 775},
      {:point, 600, 1017},
      {:text, 25, 1009},
      {:time, 1083, 1183},
      {:timestamp, 1114, 1115},
      {:timestamptz, 1184, 1185},
      {:timetz, 1266, 1270},
      {:tsrange, 3908, 3909},
      {:tstzrange, 3910, 3911},
      {:uuid, 2950, 2951},
      {:varchar, 1043, 1015}
    ]

  def is_list_type(code), do: Enum.any?(oids(), &(elem(&1, 2) == code))

  def parse_pg_array(str) do
    with {:ok, tokens, _end_line} <- str |> to_charlist() |> :pg_array_lexer.string(),
         {:ok, array} <- :pg_array_parser.parse(tokens) do
      {:ok, parse_array_values(array)}
    else
      {_, reason, _} ->
        {:error, reason}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def parse_pg_array!(str) do
    case parse_pg_array(str) do
      {:ok, array} -> array
      {:error, err} -> throw(err)
    end
  end

  def parse_array_values(array) when is_list(array), do: Enum.map(array, &parse_array_values/1)

  def parse_array_values(value) when is_binary(value) do
    Jason.decode(value)
    |> case do
      {:ok, map} -> map
      _ -> value
    end
  end

  def parse_array_values(value) when is_binary(value), do: value
end
