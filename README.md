# Pulserl - Apache Pulsar client for Erlang/Elixir

Pulserl is an Erlang client for the Apache Pulsar Pub/Sub system with both producer and consumer implementations.

### WARNING: Pulserl is currently in progress. Consider current implementations as _beta_

### Quick Examples

*The examples assume you have a running Pulsar broker at `localhost:6650`, a topic called `test-topic` (can be partitioned or not) and `rebar3` installed.*

Fetch, compile and start the erlang shell.

_Note: Pulserl uses `default` and `Exlcusive` as the default subscription name and type.
 So, if that subscription (not the consumer) under the topic `test-topic` does not exists, we make sure to create it first by creating
 the consumer before producing any message to the topic._

```
  git clone https://github.com/skulup/pulserl.git
  cd pulserl
  rebar3 shell
  rr(pulserl).  %% load the api records
  
  %% A demo function to log the value of consumed messages.
  %% This will log the value of the messages produce below 
  pulserl:start_consumption_in_background("test-topic").

  %% Asycnhrounous produce
  Promise = pulserl:produce("test-topic", "Asycn produce message").
  pulserl:await(Promise).  %% Wait broker ack
  #messageId{ledger_id = 172,entry_id = 7,
             topic = <<"persistent://public/default/test-topic">>,
             partition = -1,batch = undefined}

  %% Asycnhrounous produce. Response notification is via callback (fun/1)
  pulserl:produce("test-topic", "Hello", fun(Res) -> io:format("Response: ~p~n", [Res]) end).

  %% Synchronous produce
  pulserl:sync_produce("test-topic", "Sync produce message").
  #messageId{ledger_id = 176,entry_id = 11,
             topic = <<"persistent://public/default/test-topic">>,
             partition = -1,batch = undefined}

```
