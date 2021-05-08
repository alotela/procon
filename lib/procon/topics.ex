defmodule Procon.Topics do
  @spec create_topic(binary, pos_integer()) :: {:error, any} | :ok
  def create_topic(topic, num_partitions, options \\ []) when is_binary(topic) do
    :brod.create_topics(
      Application.get_env(:procon, :brokers),
      [
        %{
          config_entries: Keyword.get(options, :config_entries, []),
          num_partitions: num_partitions,
          replica_assignment: Keyword.get(options, :replica_assignment, []),
          replication_factor: Keyword.get(options, :replication_factor, 1),
          topic: topic
        }
      ],
      %{timeout: Keyword.get(options, :timeout, 5000)}
    )
    |> IO.inspect(label: "topic creation")
  end

  @spec create_topic(
          atom
          | %{
              optional(:brokers) => false | nil | [{atom | binary | [any], pos_integer}],
              optional(:timeout) => %{
                optional(:timeout) => pos_integer,
                optional(:validate_only) => boolean
              },
              required(:topics) => [
                [{any, any}] | %{optional(atom) => atom | binary | [any] | integer | map}
              ]
            }
        ) :: {:error, any} | :ok
  def create_topic(config) when is_map(config) do
    :brod.create_topics(
      config.brokers || Application.get_env(:procon, :brokers),
      config.topics,
      config.timeout || %{timeout: 5000}
    )
  end

  @spec delete_topic(%{
          optional(:brokers) => nil | [{atom | binary, pos_integer}],
          required(:topic) => [atom | binary],
          optional(:timeout) => pos_integer
        }) :: {:error, any} | :ok
  def delete_topic(config) do
    :brod.delete_topics(
      Map.get(config, :brokers, Application.get_env(:procon, :brokers)),
      Map.get(config, :topics),
      Map.get(config, :tiemout, 1000)
    )
  end

  def reset_topic_for_test(topic) do
    Procon.Topics.delete_topic(%{topics: [topic]})
    Procon.Topics.create_topic(topic, 1)
  end
end
