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

Add this line ```Procon.Application.after_start()``` in the ```start()``` function of the main application after ```Supervisor.start_link(children, opts)``` (be sure to still return the result of this call).

After each message enqueue, when your transaction is finished, you need to call ```Procon.MessagesProducers.ProducersStarter.start_topic_production(topic)``` for each topic you enqueued messages to start the producer.

## TODO

improve this readme
