defmodule Procon.MessagesProducers.WalDispatcherSupervisor do
  use DynamicSupervisor

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  @spec init(any) ::
          {:ok,
           %{
             extra_arguments: [any],
             intensity: non_neg_integer,
             max_children: :infinity | non_neg_integer,
             period: pos_integer,
             strategy: :one_for_one
           }}
  def init(_state) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_activated_processors_producers() do
    Procon.ProcessorConfigAccessor.activated_processors_config()
    |> Enum.map(fn {_processor_name, processor_config} ->
      Keyword.get(processor_config, :producers)
    end)
    # [
    #    %{
    #      datastore: Calions.Processors.ProcessorName.Repositories.ProcessorNamePg,
    #      relation_topics: %{
    #        "table/relation" => "topic"
    #      }
    #   }
    # ]
    |> Enum.map(fn
      %{datastore: _datastore, procon_realtime: true} = producer_config ->
        Procon.MessagesProducers.WalDispatcher.start_wal_dispatcher_for_processor(producer_config)

      %{datastore: _datastore, relation_configs: relation_configs} when relation_configs == %{} ->
        nil

      %{datastore: _datastore, relation_configs: _relation_configs} = producer_config ->
        Procon.MessagesProducers.WalDispatcher.start_wal_dispatcher_for_processor(producer_config)

      nil ->
        nil
    end)
  end
end
