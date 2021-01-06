# Procon

A high level elixir library to produce and consume kafka messages with transactionnal mechanisms

## Getting started

### Prerequisites

You need version 1.6.5 or later of elixir.

### Installation

```elixir
def deps do
  [
    {:procon, , git: "https://github.com/alotela/procon.git"}
  ]
end
```

Then run

```elixir
mix deps.get
mix procon.init
```

All information will be written in the console to use the lib.

Add this line `Procon.Application.after_start()` in the `start()` function of the main application after `Supervisor.start_link(children, opts)` (be sure to still return the result of this call).

After each message enqueue, when your transaction is finished, you need to call `Procon.MessagesProducers.ProducersStarter.start_topic_production(nb_messages_to_batch, ProcessorRepository, topic)` for each topic you enqueued messages to start the producer.

## TODO

improve this readme

## changes

- `Procon.MessagesEnqueuers.Ecto.enqueue` now return `:ok`and not anymore `{:ok, schema}`

# changelog

- le format du realtime change:

```
@type t() :: %Procon.MessagesProducers.Realtime{
          channel: String.t(),
          metadata: map(),
          session_id: String.t()
        }
```

- to enqueue a realtime automatically, in the entity config :

  - :send_realtime devient :realtime_builder
  - in config : :realtime_builder => a function returning a %Procon.MessagesProducers.Realtime{}
  - realtime_builder prend maintenant l'event_data complet et non plus juste le record enregistré (qui est donc dans event_data.event.new)
  - :rt_threshold : RT frequency/threshold for automatic rt message (if not specified, default to 1000)

- config des producers:

```
  producers: %{
    datastore: Calions.Processors.Accounts.Repositories.AccountsPg,
    topics: ["calions-int-evt-accounts"]
  }
```

devient:

```
  producers: %{
    datastore: Calions.Processors.Accounts.Repositories.AccountsPg,
    relation_topics: %{
      :accounts => {:id, :"calions-int-evt-accounts"},
      :procon_realtimes => {:id, :"procon-realtime"}
    }
  }
```

- pour tous les processerus ayant du realtime, ajouter la migration:

```
create table(:procon_realtimes) do
  add(:session_id, :string)
  add(:channel, :string)
  add(:metadata, :map, null: false, default: %{})
end
```

- dans master_key, la clef source est maintenant un atom et non plus une string
- dans les keys_mapping, tout est en atom aussi

- si aucun keys_mapping n'est spécifié, il n'y a plus de new_attributes dans la map event_data. Les attributs enregistrés sont ceux de l'event directement.

- dans la payload event_data, :attributes devient :new_attributes

```

```
