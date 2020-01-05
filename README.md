# Pulserl

Pulserl is an Erlang client for the Apache Pulsar Pub/Sub system aiming to provide a producer/consumer implementations.

### WARNING: Pulsar is currently in progress. Consider current implementations as _beta_

### Quick Examples

The examples assume you have a running Pulsar broker at `localhost:6650`, a topic called `test-topic` (can be partitioned or not) and `rebar3` installed.

```
  git clone https://github.com/alphashaw/pulserl.git
  cd pulserl
  rebar3 compile
  rebar3 shell
  {ok, Pid} = pulserl:new_producer("test-topic").
  Promise = pulserl:produce(Pid, "Asynchronous produce message").
  pulserl:await(Promise).  %% Wait broker ack
  pulserl:sync_produce(Pid, "Synchronous produce message").
<<<<<<< HEAD
  ok = pulserl:sync_produce(Pid, "{\"username\": \"1234\", \"name\":\"Johnson\", \"age\": 12, \"height\": 3}").
```
=======
```
>>>>>>> 7f7fd127589620d2a19c30193f52fcdc9a2f087d
