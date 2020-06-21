[![Build Status](https://travis-ci.com/skulup/pulserl.svg?branch=master)](https://travis-ci.com/skulup/pulserl)

# Pulserl 
#### An Apache Pulsar client for Erlang/Elixir
__Version:__ 0.1.0

Pulserl is an Erlang client for the Apache Pulsar Pub/Sub system with both producer and consumer implementations.
It requires version __2.0+__ of Apache Pulsar and __19.0+__ of Erlang 

## Quick Examples

The examples assume you have a running Pulsar broker at `localhost:6650`, a topic called `test-topic` (can be partitioned or not) and `rebar3` installed.

_Note: Pulserl uses `pulserl` and `Shared` as the default subscription name and type.
 So, if that subscription (not the consumer) under the topic `test-topic` does not exists, we make sure in this example to create it first by creating
 the consumer before producing any message to the topic._

Fetch, compile and start the erlang shell.
```
  git clone https://github.com/skulup/pulserl.git,
  cd pulserl
  rebar3 shell
```

In the Erlang shell
```erlang
  rr(pulserl).  %% load the api records
  
  %% A demo function to log the value of consumed messages
  %% that will be produced blow.
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

## Feature Matrix

 - [x] [Basic Producer](http://pulsar.apache.org/docs/en/concepts-messaging/#producers)
 - [x] [Basic Consumer](http://pulsar.apache.org/docs/en/concepts-messaging/#consumers)
 - [x] [Partitioned topics](http://pulsar.apache.org/docs/en/concepts-messaging/#partitioned-topics)
 - [x] [Batching](http://pulsar.apache.org/docs/en/concepts-messaging/#batching)
 - [ ] [Compression](http://pulsar.apache.org/docs/en/concepts-messaging/#compression)
 - [x] [TLS](https://pulsar.apache.org/docs/en/security-tls-transport/#tls-overview)
 - [ ] [Authentication (token, tls)](https://pulsar.apache.org/docs/en/security-overview/)
 - [ ] [Reader API](https://pulsar.apache.org/docs/en/concepts-clients/#reader-interface)
 - [x] [Proxy Support (for Kubernetes)](http://pulsar.apache.org/docs/en/concepts-architecture-overview/#pulsar-proxy)
 - [x] [Effectively-Once](https://pulsar.apache.org/docs/en/concepts-messaging/#deduplication-and-effectively-once-semantics)
 - [ ] [Schema](https://pulsar.apache.org/docs/en/schema-get-started/)
 - [x] Consumer seek
 - [ ] [Multi-topics consumer](https://pulsar.apache.org/docs/en/concepts-messaging/#multi-topic-subscriptions)
 - [ ] [Topics regex consumer](https://github.com/apache/pulsar/wiki/PIP-13:-Subscribe-to-topics-represented-by-regular-expressions)
 - [ ] [Compacted topics](https://pulsar.apache.org/docs/en/concepts-topic-compaction/#compaction)
 - [x] User defined properties producer/consumer
 - [ ] Reader hasMessageAvailable
 - [ ] [Hostname verification](https://pulsar.apache.org/docs/en/2.3.1/security-tls-transport/#hostname-verification)
 - [ ] [Multi Hosts Service Url support](https://pulsar.apache.org/docs/en/admin-api-overview/#java-admin-client)
 - [x] [Key_shared](https://pulsar.apache.org/docs/en/concepts-messaging/#key_shared)
 - [ ] key based batcher (didn't find a documentation) ?
 - [x] [Negative Acknowledge](https://pulsar.apache.org/docs/en/concepts-messaging/#negative-acknowledgement)
 - [x] [Delayed Delivery Messages](https://pulsar.apache.org/docs/en/concepts-messaging/#delayed-message-delivery)
 - [x] [Dead Letter Policy](https://pulsar.apache.org/docs/en/concepts-messaging/#dead-letter-topic)
 - [ ] [Interceptors](https://github.com/apache/pulsar/wiki/PIP-23:-Message-Tracing-By-Interceptors)
 
 _Thank you [Sabudaye](https://github.com/skulup/pulserl/issues/2#issuecomment-616463542) for this information_

## Overview

...

## Installation
 [Pulserl is available in Hex](https://hex.pm/packages/pulserl) for easy installation by added it to your project dependencies.

In your Erlang project's `rebar.config` 
 ```erlang
{deps, [
   {pulserl, "0.1.0"}
]}
```

In your Elixir project's `mix.exs` 
 ```elixir
def deps do
  [
    {:pulserl, "~> 0.1.0"}
  ]
end
```

## API Usage

...

## Contribute 

For issues, comments, recommendation or feedback please [do it here](https://github.com/skulup/pulserl/issues).

Contributions are highly welcome.

:thumbsup: