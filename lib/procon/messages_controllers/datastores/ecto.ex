defmodule Procon.MessagesController.Datastores.Ecto do
  import Ecto.Query

  def find_dynamic_topics(repo, topic_pattern) do
    Procon.Schemas.DynamicTopic
    |> where([pdt], like(pdt.topic_name, ^"#{topic_pattern}%"))
    |> select([pdt], pdt.topic_name)
    |> repo.all()
  end
end
