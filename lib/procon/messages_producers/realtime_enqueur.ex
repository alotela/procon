defmodule Procon.MessagesProducers.Realtime do
  def send_rtevent(%Procon.Schemas.ProconRealtime{} = event_data, repository, threshold \\ 1000) do
    key = {
      repository,
      Map.get(event_data, :channel, Map.get(event_data, :session_id, ""))
    }

    last_sent_time =
      :ets.lookup(:procon_enqueuers_thresholds, key)
      |> case do
        [{_, sent_time}] ->
          sent_time

        [] ->
          :os.system_time(:millisecond) - threshold - 1
      end

    new_sent_time = :os.system_time(:millisecond)

    case new_sent_time - last_sent_time > threshold do
      true ->
        :ets.insert(:procon_enqueuers_thresholds, {key, new_sent_time})
        repository.insert(event_data)

      false ->
        programmed_sent_time = last_sent_time + threshold

        :ets.insert(:procon_enqueuers_thresholds, {key, programmed_sent_time})

        spawn(fn ->
          Process.sleep(programmed_sent_time - new_sent_time)
          repository.insert(event_data)
        end)
    end
  end
end
