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

- ajouter instance_num: System.get_env("PROCON_INSTANCE") || 1234 à la config de procon dans le projet hote (utilisé pour générer l'ULID de trnsaction dans les messages)

## serialiaztion avro

ajouter dans la config (à terme remplacera complètement relation_topics):

```
relation_configs: %{
  account_email_validations: %{
    # avro_value_schema: "name-of-avro-schema-subjet-for-value", # by default this conforms to https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#subject-name-strategy, ie: topic-name-value"
    # avro_key_schema: "name-of-avro-schema-subjet-for-key", # by default: topic-name-key"
    # avro_value_schema_version: 1, # value schema version registered in schema registry (not schema id!!!!!)
    # avro_key_schema_version: 1, # key schema version registered in schema registry (not schema id!!!!!)
    pkey: :id,
    serialization: :avro, # :json if you want to output json
    topic: "topic-name"
  }
}
```

et dans le cas d'un consumer qui a besoin de décoder, ajouter dans le consumer l'attribut `serialization: :avro,` (par défaut il fera du json)

## schémas avro

### syntaxe d'un schéma avro

voici un exemple d'un schema avro :

```
{
  "name": "AccountEmailCreationRequest", # nom du schema
  "namespace": "io.calions", # namespace du schema
  "type": "record", # c'est un record (pourrait etre aussi un type simple genre string, mais ce n'est pas notre cas ici)
  "fields": [ # liste des champs du record, c'est un tableau
    {
      "default": null, # peut etre null par défaut, si pas présent dans le payload quand on encode
      "name": "before", # nom du champ, ici c'est un union car null | record
      "type": [ # type du champs
        "null", # peut etre null...
        { # ...mais peut etre aussi un record
          "name": "row", # nom du record, un record a toujours un nom, et ici le nom sert entre autre pour définir aussi le type du field "after" ci-dessous...
          "type": "record", # c'est un record aussi
          "fields": [
            {
              "name": "id",
              "type": { "logicalType": "UUID", "type": "string" } # les types peuvent avoir des attributs, dans ce cas plutôt que de mettre le type simple, on met une map avec les attributs
            },
            {
              "name": "email",
              "type": "string"
            },
            {
              "name": "metadata",
              "type": {
                "name": "proconMetadata",
                "type": "record",
                "fields": [
                  {
                    "name": "session_token",
                    "type": { "logicalType": "UUID", "type": "string" }
                  },
                  {
                    "name": "http_request_id",
                    "type": "string"
                  }
                ]
              }
            }
          ]
        }
      ]
    },
    { # 2eme field du schema
      "default": null,
      "name": "after",
      "type": ["null", "row"] # ici le type est encore un union, avec une référence au type "row" qu'on a défini dans le field "before" ci-dessus
    },
    {
      "name": "op",
      "type": "string"
    },
    {
      "name": "ts_ms",
      "optional": true,
      "type": "long"
    }
  ]
}
```

# confluent schema registry

To start the schema registry, different solutions exist:

- use the landoop image:
  create a docker file:

```
version: "2"

services:
  # this is our kafka cluster.
  kafka-cluster:
    image: landoop/fast-data-dev
    environment:
      ADV_HOST: 127.0.0.1 # Change to 192.168.99.100 if using Docker Toolbox
      RUNTESTS: 0 # Disable Running tests so the cluster starts faster
    ports:
      - 2181:2181 # Zookeeper
      - 3030:3030 # Landoop UI
      - 8081-8083:8081-8083 # REST Proxy, Schema Registry, Kafka Connect ports
      - 9581-9585:9581-9585 # JMX Ports
      - 9092:9092 # Kafka Broker
```

and start it:

```
docker-compose up kafka-cluster
```

you can now go to `http://127.0.0.1:3030/` and manage the cluster.

- you can start the schema registry using confluent binary in your confluent platform folder (not kafka archive, but confluent platform):
  `./bin/schema-registry-start ./etc/schema-registry/schema-registry.properties`

You need to download confluent platform archive (it's free). If you download kafka archive, it won't contain schema registry binary.

You also need confluent archive to have avro producer/consuler binaries.
