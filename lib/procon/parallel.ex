defmodule Procon.Parallel do
  def pmap(collection, func, timeout \\ 5000) do
    collection
    |> Enum.map(&Task.async(fn -> func.(&1) end))
    |> Task.await_many(timeout)
  end
end
