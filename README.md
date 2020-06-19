# Pulserl 
#### An Apache Pulsar client for Erlang/Elixir
__Version:__ 0.1.0

Pulserl is an Erlang client for the Apache Pulsar Pub/Sub system with both producer and consumer implementations.
It requires version __2.0+__ of Apache Pulsar and __19.0+__ of Erlang 

## Quick Examples

The examples assume you have a running Pulsar broker at `localhost:6650`, a topic called `test-topic` (can be partitioned or not) and `rebar3` installed.

Fetch, compile and start the erlang shell.

_Note: Pulserl uses `default` and `Exlcusive` as the default subscription name and type.
 So, if that subscription (not the consumer) under the topic `test-topic` does not exists, we make sure in this example to create it first by creating
 the consumer before producing any message to the topic._

```
  git clone https://github.com/skulup/pulserl.git
  cd pulserl
  rebar3 shell
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

 - [x] Partitioned topics 
 - [x] Batching	
 - [ ] Compression
 - [x] TLS	
 - [ ] Authentication (soon)	
 - [ ] Reader API	
 - [x] Proxy Support				
 - [ ] Effectively-Once				
 - [ ] Schema				
 - [x] Consumer seek				
 - [ ] Multi-topics consumer (soon)					
 - [ ] Topics regex consumer (soon)					
 - [ ] TCompacted topics						
 - [x] User defined properties producer/consumer						
 - [ ] Reader hasMessageAvailable						
 - [ ] Hostname verification (soon)						
 - [ ] Multi Hosts Service Url support						
 - [x] Key_shared					
 - [ ] key based batcher						
 - [x] Negative Acknowledge							
 - [x] Delayed Delivery Messages						
 - [x] Dead Letter Policy							
 - [ ] Interceptors	(soon)		
 
 
## Overview