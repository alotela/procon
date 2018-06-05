# Procon

A high level elixir library to produce and consume kafka messages with transactionnal mechanisms

## Installation

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

and add this line ```Procon.MessagesProducers.ProducersStarter.start_topics_production_from_database_messages()``` in the ```start()``` function of the main application.

## TODO

improve this readme
