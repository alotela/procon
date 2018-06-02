defmodule Procon.MessagesProducers.ResourcesRegistryGenServer do
  use GenServer

  def start_link(opts) do
    case Keyword.has_key?(opts, :name) do
      true -> GenServer.start_link(__MODULE__, nil, opts)
      _ -> {:error, :options_must_contain_name_attribute} 
    end
  end

  def whereis_resource(registry_name, resource_name) 
  when is_atom(registry_name) do
    GenServer.call(registry_name, {:whereis_resource, resource_name})
  end

  def register_resource(registry_name, resource_name, pid)
  when is_atom(registry_name) and is_pid(pid) do
    GenServer.call(registry_name, {:register_resource, resource_name, pid})
  end

  def unregister_resource(registry_name, resource_name)
  when is_atom(registry_name) do
    GenServer.cast(registry_name, {:unregister_resource, resource_name})
  end

  def send_message_to_resource(registry_name, resource_name, message)
  when is_atom(registry_name) do
    case whereis_resource(registry_name, resource_name) do
      :undefined ->
        {:unkown_resource, {resource_name, message}}

      pid ->
        Kernel.send(pid, message)
        pid
    end
  end

  # SERVER

  def init(_) do
    {:ok, Map.new()}
  end

  def handle_call({:whereis_resource, resource_name}, _from, state) do
    {:reply, Map.get(state, resource_name, :undefined), state}
  end

  def handle_call({:register_resource, resource_name, pid}, _from, state) do
    case Map.get(state, resource_name) do
      nil ->
        {:reply, :registered, Map.put(state, resource_name, pid)}

      _ ->
        {:reply, :already_registered, state}
    end
  end

  def handle_cast({:unregister_resource, resource_name}, state) do
    {:noreply, Map.delete(state, resource_name)}
  end
end
