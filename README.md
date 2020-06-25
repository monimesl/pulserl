[![Build Status](https://travis-ci.com/skulup/pulserl.svg?branch=master)](https://travis-ci.com/skulup/pulserl)
[![Language](https://img.shields.io/badge/Language-Erlang-b83998.svg)](https://www.erlang.org/)
[![LICENSE](https://img.shields.io/badge/License-Apache%202-blue.svg)](https://github.com/skulup/pulserl/blob/master/LICENSE)
# Pulserl 
#### An Apache Pulsar client for Erlang/Elixir
__Version:__ 0.1.0

Pulserl is an Erlang client for the Apache Pulsar Pub/Sub system with both producer and consumer
implementations. It requires version __2.0+__ of Apache Pulsar and __18.0+__ of Erlang. 
Pulserl uses the [binary protocol](http://pulsar.apache.org/docs/en/develop-binary-protocol)
to interact with the Pulsar brokers and exposes a very simple API. 
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
 

## Installation
 [Pulserl is available in Hex](https://hex.pm/packages/pulserl) for easy installation by added it to your project dependencies.

In your Erlang project's `rebar.config` 
 ```erlang
{deps, [
   {pulserl, "<latest-version>"}
]}
```

In your Elixir project's `mix.exs` 
 ```elixir
def deps do
  [
    {:pulserl, "~> <latest-version>"}
  ]
end
```

## API Usage

### Client Setup
  In pulserl, the client as of now (for API simplicity) is a singleton (local registered `gen_server`) 
  and can be created during startup by the `application controller` or on demand at a later time.
  The client has the responsibility of creating the TCP connections,
  maintaining the connection pool, and ensures these connections are 
  maximally used by the producers and consumers. The client is also responsible
  for querying metadata needed to initialize a producer or consumer; it does this
  by creating a metadata socket during initialization by using the provided configurations.

  #### Automatic client startup
   You can configure the client that will be auto-started by providing
   the following configuration for `pulserl` in your `sys.config` file.
```erlang
[
  {pulserl, [
    {autostart, true} %% If false, the client will be created on startup. Default is true.
    %% The TCP connect timeout in milliseconds. Default is 30000.
    , {connect_timeout_ms, 30000}
    %% The maximum connections to each broker the client should create.
    %% Default is 1. Increasing this may improve I/O throughput
    , {max_connections_per_broker, 1}
    %% The underlying TCP socket options.
    %% https://erlang.org/doc/man/gen_tcp.html#type-connect_option
    , {socket_options, [{nodelay, true}]}
    %% The service url. Default is the non TLS url: "pulsar://${hostname}:6650"
    , {service_url, "pulsar+ssl://localhost:6651/"}
    %% The trust certificate file path. Required only if the TLS service url is used.
    %% See http://pulsar.apache.org/docs/en/security-tls-transport/
    , {tls_trust_certs_file, "/path/to/cacert.pem"}
  ]}
].
```  
  #### On demand client startup
  The `pulserl:start_client/1,2` API can be used to start the pulserl client when needed. 
```erlang
 ServiceUrl = "pulsar+ssl://localhost:6651/",
 Config = #clientConfig{
             connect_timeout_ms = 30000,
             max_connections_per_broker = 1,
             socket_options = [{nodelay, true}],
             tls_trust_certs_file = "/path/to/cacert.pem"
           },
 ok = pulserl:start_client(ServiceUrl, ClientConfig).
```  

### Producer 
Pulserl creates a `gen_server` process per topic. For a topic of `n` partition, it creates
a parent producer under the `pulserl_producer_sup` supervision tree which in turn `start_link`
and manage `n` child producers. The parent producer serves as a facade to the internal producers.
The parent monitor the child processes (internal partitioned producers) for resilience, 
route client calls to one of the child processes using different 
[routing modes](https://pulsar.apache.org/docs/en/concepts-messaging/#routing-modes). 
A producer during initialization is assigned a connection by the client based on its topic metadata.
The producers uses a queueing mechanism on message sending. 
Each send is internally a `gen_server.call/2` to the producer process. The caller is added to 
a queue and replied immediately with `ok.` This initial early reply frees up the caller to do 
other tasks if the response is not needed immediately. Internally if message send is trigger, i.e
when batching is not enable or batching enabled and a batch send is triggered, the producer
asynchronously send (`gen_sever.cast/2`) the message(s) to the `pulserl_conn` process. When the
connection process receives the response it will `!` send it to associated the producer which in 
turn dequeue the associated caller and reply to it.
The producer provides synchronous and asynchronous send API.


In synchronous mode, the call will wait for the broker to acknowledge the message. 
If the acknowledgment is not received and a `send_timeout` is specified, a `{error, send_timeout}`
is sent to client on timed out. 

The asynchronous mode provides two API. One returns a `reference()` that will be used to probe 
for a response or error. The other allows one to pass a callback `fun/1` that will be invoke 
internally by the producer process when there is a response or error.

#### Starting a producer
...

## Contribute 

For issues, comments, recommendation or feedback please [do it here](https://github.com/skulup/pulserl/issues).

Contributions are highly welcome.

:thumbsup: