defmodule Procon.MessagesProducers.EpgsqlConnector do
  defmodule Config do
    defstruct database: "",
              host: 'localhost',
              password: "",
              replication: "database",
              slot: nil,
              username: ""

    @type t() :: %__MODULE__{
            database: String.t(),
            host: charlist(),
            password: String.t(),
            replication: String.t(),
            slot: atom(),
            username: String.t()
          }
  end

  def connect(%Config{} = config, tables) do
    case :epgsql.connect(Map.take(config, [:database, :host, :password, :replication, :username])) do
      {:ok, epgsql_pid} ->
        create_publication(epgsql_pid, config.database, tables)

        {:ok, slot_name} =
          create_replication_slot(
            epgsql_pid,
            Map.get(config, :slot, :temporary)
          )

        {:ok, %{epgsql_pid: epgsql_pid, replication_slot_name: slot_name}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def disconnect(epgsql_pid) do
    :ok = :epgsql.close(epgsql_pid)
    :ok
  end

  def start_replication(epgsql_pid, slot_name, xlog, offset, publication_names) do
    :ok =
      :epgsql.start_replication(
        epgsql_pid,
        slot_name,
        self(),
        [],
        '#{xlog}/#{offset}',
        'proto_version \'1\', publication_names \'#{publication_names}\''
      )

    :ok
  end

  def acknowledge_lsn(epgsql, lsn) do
    :epgsql.standby_status_update(epgsql, lsn, lsn)
  end

  defp create_publication(epgsql_pid, database, tables) when is_list(tables) do
    :epgsql.squery(
      epgsql_pid,
      "CREATE PUBLICATION procon_#{database} FOR TABLE #{tables |> List.first()};"
    )

    tables
    |> Enum.map(fn table ->
      # {:ok, _, _} =
      :epgsql.squery(
        epgsql_pid,
        "ALTER PUBLICATION procon_#{database} ADD TABLE #{table};"
      )
    end)
  end

  @spec get_primary_key_columns(pid, list()) :: %{String.t() => integer()}
  def get_primary_key_columns(epgsql_pid, table_names) do
    {:ok, _, tuples_list} =
      :epgsql.squery(
        epgsql_pid,
        "SELECT
          t.relname as table_name,
          a.attnum as pkey_column
        FROM
          pg_class t
        JOIN
          pg_attribute a on a.attrelid = t.oid
        JOIN
          pg_index ix on t.oid = ix.indrelid AND a.attnum = ANY(ix.indkey)
        WHERE
          t.relname in ( '#{table_names |> Enum.join("', '")}' )
        AND
          indisprimary
        "
      )

    tuples_list
    |> Enum.reduce(
      %{},
      fn {table_name, pkey_index}, map ->
        Map.put(
          map,
          table_name,
          String.to_integer(pkey_index)
        )
      end
    )
  end

  defp create_replication_slot(epgsql_pid, slot) do
    {slot_name, start_replication_command} =
      case slot do
        :temporary ->
          slot_name = self_as_slot_name()

          {slot_name,
           "CREATE_REPLICATION_SLOT #{slot_name} TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT"}

        name when is_atom(name) ->
          # Simple query for replication mode so no prepared statements are supported
          [_elixir, scope, _processors, processor_type, processor_name | _rest] =
            name
            |> Atom.to_string()
            |> String.split(".")

          escaped_name =
            [scope, processor_type |> String.first(), processor_name]
            |> Enum.join("_")
            |> String.downcase()

          # escaped_name = String.downcase(String.replace(Atom.to_string(name), ".", "_"))

          {:ok, _, [{existing_slot}]} =
            :epgsql.squery(
              epgsql_pid,
              "SELECT COUNT(*) >= 1 FROM pg_replication_slots WHERE slot_name = '#{escaped_name}'"
            )

          case existing_slot do
            "t" ->
              # no-op
              {escaped_name, "SELECT 1"}

            "f" ->
              {escaped_name,
               "CREATE_REPLICATION_SLOT #{escaped_name} LOGICAL pgoutput NOEXPORT_SNAPSHOT"}
          end
      end

    case :epgsql.squery(epgsql_pid, start_replication_command) do
      {:ok, _, _} ->
        {:ok, slot_name}

      {:error, {:error, :error, "42710", :duplicate_object, _error_message, _error_detail}} ->
        {:ok, slot_name}

      {:error, epgsql_error} ->
        {:error, epgsql_error}
    end
  end

  # TODO: Replace with better slot name generator
  defp self_as_slot_name() do
    "#PID<" <> pid = inspect(self())

    pid_number =
      String.replace(pid, ".", "_")
      |> String.slice(0..-2)

    "pid" <> pid_number
  end
end
