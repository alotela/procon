defmodule Procon.Listener do
  use GenServer

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Procon.Listener)

  @impl true
  def init(_opts) do
    {:ok, pid} = Postgrex.Notifications.start_link(database: "calions_accounts_dev")
    {:ok, reference} = Postgrex.Notifications.listen(pid, "accounts_changed")
    {:ok, %{reference: reference, pid: pid}}
  end

  require Logger

  @impl true
  def handle_info({:notification, _pid, _ref, "accounts_changed", payload}, _state) do
    with {:ok, data} <- Jason.decode(payload, keys: :atoms) do
      data
      |> inspect()
      |> Logger.info()

      {:noreply, :event_handled}
    else
      error -> {:stop, error, []}
    end
  end
end
