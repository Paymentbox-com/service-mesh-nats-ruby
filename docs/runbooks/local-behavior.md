# Local Behavior Runbook

This runbook checks the transport's behavior by hand against a local
`nats-server` with JetStream. Each check runs a receiving process in one
terminal and a sending process in another, and lists the output that shows
the behavior. The output below comes from real runs.

Three small programs in `docs/runbooks/bin/` do the work. They load the gem
from the checkout, so they run the current code with no install.

| Program | Does |
|---|---|
| `serve.rb` | Runs one endpoint or subscriber and prints each message its handler receives, with the time and its metadata. It also prints a JetStream subscriber's consumer settings as the server holds them, and calls `stop` on Ctrl-C. |
| `send.rb` | Sends requests or publishes and prints each result, naming the error class when there is one. |
| `streams.rb` | Creates, inspects, and deletes streams and consumers. |

The commands below use these aliases, set once in each terminal from the
repository root:

```sh
alias serve='bundle exec ruby docs/runbooks/bin/serve.rb'
alias send='bundle exec ruby docs/runbooks/bin/send.rb'
alias streams='bundle exec ruby docs/runbooks/bin/streams.rb'
```

`serve` and `send` take the settings the library reads as repeatable
`key=value` flags, one flag for each place a setting can live:

| Flag | Sets | Program |
|---|---|---|
| `--cfg` | The config given to `Client.new` and `Runtime.new` | Both |
| `--target` | The `Target`'s metadata | Both |
| `--set` | The `Endpoint`'s or `Subscriber`'s metadata in `serve`, and the per-call options in `send` | Both |
| `--md` | The message metadata | `send` |

These are the other flags.

| Program | Flag | Default | Meaning |
|---|---|---|---|
| Both | `--kind` | `topic` | `route` for an endpoint and `request`, `topic` for a subscriber and `publish`. |
| Both | `--subject` | `demo.created` | The target's subject. |
| `serve` | `--group` | none | The `consumer_group` field: unset for the deployment group, `none`, or a name. |
| `serve` | `--fail N` | `0` | Fail the first N handler calls. |
| `serve` | `--work SECONDS` | `0` | How long each handler call runs. |
| `serve` | `--drain SECONDS` | `10` | The drain budget `stop` gets on Ctrl-C. |
| `send` | `--n` | `1` | How many messages to send. |
| `send` | `--payload` | `hello` | The payload. Each message appends its number. |
| `send` | `--stream-info` | off | After sending, print the target's stream from `client.stream_info`. |

`serve` uses the deployment group `demo` unless `--cfg deployment_group=...`
sets another. `NATS_URL` points all three programs at a server other than
`nats://127.0.0.1:4222`.

## Shared Conventions

The programs follow conventions that runbook programs in other languages'
transports can follow too, so either half of a check can run in another
language against this one:

- The default subject is `demo.created`, and the default deployment group is
  `demo`.
- `send` sends `<payload> <n>`, with `hello` as the default payload.
- `serve`'s endpoint replies with the payload `echo: <payload>` and the
  metadata `Served-By: <pid>`.
- `serve --fail N` fails with `failing call <n> of <N> on purpose`, which an
  endpoint's caller sees in `Mesh-Handler-Error`.
- `streams add-consumer` creates a durable consumer with explicit acks and a
  60-second ack wait.

Setting values stay in each library's own form. This transport reads
durations as seconds, such as `2` or `0.5`.

## Terminal 1, the Server

```sh
brew install nats-server   # once
nats-server -js -sd "$(mktemp -d)"
```

`-js` turns on JetStream, and `-sd` keeps its storage in a fresh temporary
directory, so every session starts with no streams and no consumers.

The checks below run in terminals 2 and 3, and sometimes 4 and 5. Each
`serve` keeps running until Ctrl-C.

## Requests

### Mesh-Timeout and Mesh-Deadline

```sh
# terminal 2
serve --kind route --subject demo.echo
# terminal 3
send --kind route --subject demo.echo --cfg request_timeout=2
send --kind route --subject demo.echo --set request_timeout=0.5
```

Terminal 3 prints `reply "echo: hello 1" metadata {"Served-By"=>"<pid>"}` both
times. Terminal 2 shows what the handler received:

```
18:40:48.928 call 1: "hello 1"
    Mesh-Deadline: 2026-11-06T01:40:50.928125000Z
    Mesh-Timeout: 2000
18:40:49.079 call 2: "hello 1"
    Mesh-Deadline: 2026-11-06T01:40:49.579467000Z
    Mesh-Timeout: 500
```

`Mesh-Deadline` is the request's arrival plus `Mesh-Timeout`.

### Where request_timeout Is Read From

```sh
# terminal 2
serve --kind route --subject demo.slow --work 3
# terminal 3
send --kind route --subject demo.slow --target request_timeout=0.5
send --kind route --subject demo.slow --target request_timeout=0.5 --set request_timeout=4
```

The first prints `NATS::IO::Timeout: nats: timeout after 505ms`, because the
target's setting applies. The second prints a reply after about 3s, because
the per-call option outranks the target.

### Request Errors

```sh
# terminal 3, with nothing serving demo.none
send --kind route --subject demo.none
# terminal 2
serve --kind route --subject demo.fail --fail 1
# terminal 3
send --kind route --subject demo.fail --n 2
send --kind route --subject demo.fail --set stream=X
```

| Command | Prints |
|---|---|
| Nothing serving | `NATS::IO::NoRespondersError` |
| `--fail 1`, first request | `HandlerError "failing call 1 of 1 on purpose"` |
| `--fail 1`, second request | `reply "echo: hello 2"` |
| `--set stream=X` | `BadConfig: stream="X" applies to topic targets only` |

Terminal 2 logs the failure:

```
ERROR service_mesh_nats: endpoint handler failed subject=demo.fail error=RuntimeError: failing call 1 of 1 on purpose
```

## Core Consumer Groups

```sh
# terminals 2, 4, and 5
serve --subject demo.core --cfg deployment_group=billing
serve --subject demo.core --cfg deployment_group=audit
serve --subject demo.core --cfg deployment_group=audit
# terminal 3
send --subject demo.core --n 4
```

`billing` handles all four messages. The two `audit` instances share a queue
group and handle four between them, such as one and three.

Adding `--group none` to the two `audit` instances makes each of them handle
all four. Adding `--group shared` to all three makes the three share one
group across both deployments, so the four messages are spread across all
three instances.

## Configuration Mistakes

Each of these exits right away with `BadConfig` and the key and value.

```sh
send --cfg request_timeout=soon
serve --cfg concurrency=many
serve --cfg max_deliver=0
serve --set stream=DEMO_CREATED --set ack_policy=all
serve --group none --set stream=DEMO_CREATED --set create_consumer=false
serve --cfg pull_expiry=soon
serve --set stream=DEMO_CREATED --set ack_wait=2m
```

They print:

```
Client.new: ServiceMeshNats::BadConfig: request_timeout="soon" must be a positive number of seconds
Runtime.new: ServiceMeshNats::BadConfig: concurrency="many" must be a positive integer
Runtime.new: ServiceMeshNats::BadConfig: max_deliver="0" must be a positive integer
Runtime.new: ServiceMeshNats::BadConfig: ack_policy="all" must be one of "double", "explicit", "none"
Runtime.new: ServiceMeshNats::BadConfig: create_consumer="false" needs a named consumer group
Runtime.new: ServiceMeshNats::BadConfig: pull_expiry="soon" must be a positive number of seconds
Runtime.new: ServiceMeshNats::BadConfig: ack_wait="2m" must be a positive number of seconds
```

The last one shows that durations are seconds, so `2m` does not parse.

## Streams

The JetStream checks use one stream for the subject `demo.created`:

```sh
streams add DEMO_CREATED demo.created
```

It prints `created stream "DEMO_CREATED" for ["demo.created"] with limits retention`.
`streams add NAME SUBJECT interest` or `workqueue` creates a stream with that
retention instead.

## The Boot Check

```sh
streams add BAD demo.echo
serve --kind route --subject demo.echo
streams rm BAD
```

`serve` exits with
`start: ServiceMeshNats::StreamCoversRoute: a stream covers an endpoint subject: subject "demo.echo", stream "BAD"`.
After `rm`, the same `serve` starts.

## JetStream Publishing

With nothing subscribed:

```sh
send --set stream=DEMO_CREATED
send --set stream=DEMO_CREATED --n 2 --md Mesh-Message-Id=same-id
streams info DEMO_CREATED
send --subject demo.created --set stream=OTHER
send --subject demo.nostream --set stream=NOPE
```

| Command | Prints |
|---|---|
| A publish to the stream | `publish 1: ok` |
| Two publishes with the same `Mesh-Message-Id` | `ok` twice, while `streams info` shows only one more message, because the stream dropped the repeat |
| The subject is in a different stream | `NATS::JetStream::Error::BadRequest: expected stream does not match (status_code=400, err_code=10060)` |
| No stream holds the subject | `NATS::JetStream::Error::NoStreamResponse: nats: no response from stream` |

With a core subscriber on the subject and no stream, the publish waits for an
ack that never comes:

```sh
# terminal 2
serve --subject demo.listen
# terminal 3
send --subject demo.listen --set stream=NOPE --set request_timeout=1
```

It prints `NATS::IO::Timeout: nats: timeout after 1005ms`.

`client.stream_info` reads only the target's metadata:

```sh
send --set stream=DEMO_CREATED --stream-info
send --target stream=DEMO_CREATED --stream-info
```

The first prints `stream_info: the target has no stream setting`. The second
prints `stream "DEMO_CREATED": 4 messages, last sequence 4, 0 consumers`.

## JetStream Subscribing

Each check below uses its own consumer group, so the checks don't share a
consumer. `--set deliver_policy=new` keeps a new consumer from replaying what
earlier checks published.

### A Named Group's Consumer and Its Settings

```sh
# terminal 2
serve --group audit --set stream=DEMO_CREATED --set deliver_policy=new --set ack_wait=10 --fail 1
# terminal 3
send --set stream=DEMO_CREATED --md Mesh-Message-Id=abc
```

At start, terminal 2 prints the consumer as the server holds it:

```
serving topic demo.created in consumer group audit
consumer audit on stream DEMO_CREATED
    ack_policy: explicit
    ack_wait: 10.0s
    max_deliver: 5
    max_ack_pending: 1000
    deliver_policy: new
    inactive_threshold: 604800.0s
    num_replicas: 0
```

`max_deliver: 5` comes from the runtime's default, and the 7-day
`inactive_threshold` is the named-group default. `num_replicas: 0` means the
consumer takes the stream's replica count. The first call fails and is
nacked, and the message comes again right away:

```
18:35:32.036 call 1: "hello 1"
    Mesh-Deadline: 2026-11-06T01:35:42.036050000Z
    Mesh-Delivery-Attempt: 1
    Mesh-Message-Id: abc
    Mesh-Nats-Consumer: audit
    Mesh-Nats-Consumer-Sequence: 1
    Mesh-Nats-Num-Pending: 0
    Mesh-Nats-Stream: DEMO_CREATED
    Mesh-Nats-Stream-Sequence: 5
    Mesh-Nats-Timestamp: 2026-11-06T01:35:32.035631000Z
    Nats-Expected-Stream: DEMO_CREATED
    Nats-Msg-Id: abc
ERROR service_mesh_nats: subscriber handler failed subject=demo.created stream=DEMO_CREATED delivery_attempt=1 error=RuntimeError: failing call 1 of 1 on purpose
18:35:32.036 call 2: "hello 1"
    Mesh-Deadline: 2026-11-06T01:35:42.036578000Z
    Mesh-Delivery-Attempt: 2
    ...
```

`Mesh-Deadline` is the delivery's arrival plus `ack_wait`. The publish's own
headers, `Nats-Expected-Stream` and `Nats-Msg-Id`, reach the handler as
metadata too. `streams consumers DEMO_CREATED` afterwards shows the consumer
with `ack pending 0`.

### Instances of One Group Split Messages

```sh
# terminals 2 and 4
serve --group split --set stream=DEMO_CREATED --set deliver_policy=new
# terminal 3
send --set stream=DEMO_CREATED --n 10
```

Each instance handles some of the ten, such as 1, 3, 5, 7, and 9 in one and
the rest in the other, and no message reaches both. A third `serve` with
`--group other` handles all ten.

### Ack Policies

`double` and `explicit` create the same consumer on the server, with NATS's
explicit ack policy. They differ in how the runtime sends each ack: `double`
sends it as a request and waits for the server to confirm it, and `explicit`
publishes it without waiting. A plain subscriber on the stream's ack subjects
shows the difference, because only a request carries a reply subject. This
is the one check that uses the `nats` command-line tool, installed with
`brew install nats-io/nats-tools/nats`.

```sh
# terminal 4
nats sub '$JS.ACK.DEMO_CREATED.>'
# terminal 2
serve --group ack-double --set stream=DEMO_CREATED --set deliver_policy=new --set ack_policy=double
# terminal 3
send --set stream=DEMO_CREATED
```

Terminal 4 prints the ack with a reply subject:

```
[#1] Received on "$JS.ACK.DEMO_CREATED.ack-double.1.16.1.1793928945413221000.0" with reply "_INBOX.Av4QpPUUE2WiYQAntriuT7.Av4QpPUUE2WiYQAntriumH"
+ACK
```

The same steps with `--group ack-explicit --set ack_policy=explicit` print
the ack with no reply subject:

```
[#2] Received on "$JS.ACK.DEMO_CREATED.ack-explicit.1.17.1.1793928948194114000.0"
+ACK
```

`none` on a named group acks nothing, so delivery is at most once:

```sh
# terminal 2
serve --group once --set stream=DEMO_CREATED --set deliver_policy=new --set ack_policy=none --fail 1
# terminal 3
send --set stream=DEMO_CREATED
```

Terminal 2 prints the consumer with `ack_policy: none` and `max_deliver: -1`,
then one `call 1` with no `Mesh-Deadline`, which fails and is not delivered
again. Terminal 4, if still running, prints no ack.

`none` on a subscriber whose consumer group is `none` gives an ordered
consumer, as described under
[none Consumers and Ordered Consumers](#none-consumers-and-ordered-consumers).

### Retries: max_deliver, nak_delay, and backoff

```sh
# terminal 2
serve --group retry --set stream=DEMO_CREATED --set deliver_policy=new \
  --set max_deliver=3 --set nak_delay=1 --fail 99
# terminal 3
send --set stream=DEMO_CREATED
```

The message arrives three times, a second apart, and then stops with the
final-failure error:

```
18:36:04.142 call 1: "hello 1"
    Mesh-Delivery-Attempt: 1
18:36:05.144 call 2: "hello 1"
    Mesh-Delivery-Attempt: 2
18:36:06.146 call 3: "hello 1"
    Mesh-Delivery-Attempt: 3
ERROR service_mesh_nats: message will not be delivered again subject=demo.created stream=DEMO_CREATED consumer=retry stream_sequence=19 delivery_attempt=3 max_deliver=3
```

A failed handler's message is nacked, and a nak without `nak_delay` sends it
again right away. `backoff` applies only when a delivery is not settled before
its deadline, so it shows with a handler that runs past its deadline:

```sh
# terminal 2
serve --group bo --set stream=DEMO_CREATED --set deliver_policy=new \
  --set backoff=2,10 --set max_deliver=3 --work 30 --drain 1
# terminal 3
send --set stream=DEMO_CREATED
```

The consumer prints `ack_wait: 2.0s` and `backoff: ["2.0s", "10.0s"]`,
because the server sets `ack_wait` to the first `backoff` entry. The message
arrives again 2s after the first delivery and 10s after the second, while the
earlier handlers are still running. Each `Mesh-Deadline` is its delivery's
arrival plus its `backoff` entry:

```
18:36:09.444 call 1: "hello 1"
    Mesh-Deadline: 2026-11-06T01:36:11.443902000Z
    Mesh-Delivery-Attempt: 1
18:36:11.446 call 2: "hello 1"
    Mesh-Deadline: 2026-11-06T01:36:21.446035000Z
    Mesh-Delivery-Attempt: 2
18:36:21.446 call 3: "hello 1"
    Mesh-Deadline: 2026-11-06T01:36:31.446034000Z
    Mesh-Delivery-Attempt: 3
```

Ctrl-C then ends the 1s drain with all three handlers still running, and each
of the three deliveries is nacked with `handler_running=true`. The same
command with `--fail 99` in place of `--work 30` delivers all three right
away, because each failure is nacked before its deadline.

### Consumer Settings the Server Won't Change

The runtime creates or updates a named group's consumer at every `start`. The
server refuses some changes to a consumer after it is created, and
`deliver_policy` is one of them.

```sh
serve --group fixed --set stream=DEMO_CREATED --set deliver_policy=new
# Ctrl-C, then start the same group without deliver_policy=new
serve --group fixed --set stream=DEMO_CREATED
```

The second exits with
`start: NATS::JetStream::Error::ServerError: deliver policy can not be updated (status_code=500, err_code=10012)`.

### none Consumers and Ordered Consumers

```sh
serve --group none --set stream=DEMO_CREATED --set deliver_policy=new
serve --group none --set stream=DEMO_CREATED --set ack_policy=none
```

The first gets its own consumer with a name the runtime generates, which
acks and has the server's 5-second `inactive_threshold`. The second is an
ordered consumer, kept in memory with one replica:

```
consumer Xm2W92306ppomuEPLBSQSf on stream DEMO_CREATED
    ack_policy: none
    max_deliver: -1
    deliver_policy: all
    inactive_threshold: 300.0s
    mem_storage: true
    num_replicas: 1
```

With `deliver_policy=all` it replays the whole stream, and its handlers get
no `Mesh-Deadline`. Each start creates a new consumer for both commands.

An ordered consumer that disappears is replaced. With the ordered `serve`
running, deleting its consumer by the name it printed:

```sh
streams rm-consumer DEMO_CREATED CJ9dT60BuuIwa1mMedKxLP
send --set stream=DEMO_CREATED
```

makes terminal 2 print:

```
WARN service_mesh_nats: replacing the ordered consumer subject=demo.created stream=DEMO_CREATED consumer=CJ9dT60BuuIwa1mMedKxLP reason=the server reported the consumer deleted
18:40:53.346 call 1: "hello 1"
    Mesh-Delivery-Attempt: 1
    Mesh-Nats-Consumer: CJ9dT60BuuIwa1mMedKxMc
    Mesh-Nats-Consumer-Sequence: 1
```

The message arrives through the new consumer, whose sequence starts again at
1.

### A Consumer Managed Outside the Runtime

```sh
streams add-consumer DEMO_CREATED billing demo.created
serve --group billing --set stream=DEMO_CREATED --set create_consumer=false --set ack_wait=5
serve --group nobody --set stream=DEMO_CREATED --set create_consumer=false
```

The first `serve` prints `ack_wait: 60.0s`, which is the consumer's own
setting, because the runtime ignores `--set ack_wait=5` for a consumer it
doesn't create. Each `Mesh-Deadline` is 60 seconds after its arrival. The
second exits with
`start: NATS::JetStream::Error::ConsumerNotFound: consumer not found (status_code=404, err_code=10014)`.

### Target Settings and Subscriber Overrides

```sh
serve --group layer --target stream=MISSING --set stream=DEMO_CREATED
serve --group layer2 --target stream=MISSING
```

The first consumes from `DEMO_CREATED`, because the subscriber's own setting
outranks the target's. The second exits with
`start: NATS::JetStream::Error::StreamNotFound: stream not found (status_code=404, err_code=10059)`.

### Pulls and concurrency

A subscriber keeps up to `concurrency` messages owed by the server or waiting
for a handler, on top of the ones its handlers are running. With handlers
that hold every slot, that number shows as ack pending, because the server
counts every message the subscriber has taken as unacked.

```sh
# terminal 2
serve --group pull1 --set stream=DEMO_CREATED --set deliver_policy=new \
  --cfg concurrency=1 --work 60 --set ack_wait=60 --drain 1
# terminal 3
send --set stream=DEMO_CREATED --n 20
streams consumers DEMO_CREATED
```

With `concurrency=1`, one message runs and one waits:

```
consumer "pull1": pending 18, ack pending 2, redelivered 0, ...
```

With `--group pull4 --cfg concurrency=4`, four run and three wait, because
the subscriber pulls for the difference once fewer than half of
`concurrency` are owed or waiting:

```
consumer "pull4": pending 13, ack pending 7, redelivered 0, ...
```

Ctrl-C ends each 1s drain and naks every message the subscriber held, 2 and
7 of them.

`pull_expiry` and `pull_heartbeat` go to the server with each pull. Their
limits are checked at `start`:

```sh
serve --group pull-bad --set stream=DEMO_CREATED --set pull_expiry=0.5
serve --group pull-bad --set stream=DEMO_CREATED --set pull_heartbeat=20
```

They exit with
`start: ServiceMeshNats::BadConfig: pull_expiry=0.5 must be at least 1 second`
and
`start: ServiceMeshNats::BadConfig: pull_heartbeat=20.0 must be no more than half of pull_expiry=30.0`.

### A Missing Consumer

```sh
# terminal 2
serve --group gone --set stream=DEMO_CREATED --set deliver_policy=new
# terminal 3
streams rm-consumer DEMO_CREATED gone
# 10 seconds later
streams add-consumer DEMO_CREATED gone demo.created
```

Terminal 2 logs the missing consumer right away and every 5 seconds after,
and keeps logging after the consumer is back, because a consumer deleted
while connected is not pulled again until the next `start`:

```
ERROR service_mesh_nats: consumer missing subject=demo.created stream=DEMO_CREATED consumer=gone
ERROR service_mesh_nats: consumer missing subject=demo.created stream=DEMO_CREATED consumer=gone
ERROR service_mesh_nats: consumer missing subject=demo.created stream=DEMO_CREATED consumer=gone
ERROR service_mesh_nats: consumer missing subject=demo.created stream=DEMO_CREATED consumer=gone
```

## Stopping

`serve` starts first, so its consumer exists before the publish. With
`deliver_policy=new`, a consumer created after the publish never sees it. The
message ID differs on every run, because the stream drops a repeated
`Mesh-Message-Id` within its two-minute duplicate window without an error.

```sh
# terminal 2
serve --group drain2 --set stream=DEMO_CREATED --set deliver_policy=new --work 30 --drain 1
# terminal 3, once terminal 2 prints "serving"
send --set stream=DEMO_CREATED --md Mesh-Message-Id=drain-$(date +%s)
# terminal 2, within 30s of "call 1", while the handler runs: Ctrl-C
```

The drain ends before the handler, so `stop` naks the message and returns
`false`:

```
stopping with a drain of 1.0s
WARN service_mesh_nats: nacked a message at the end of the drain subject=demo.created stream=DEMO_CREATED consumer=drain2 stream_sequence=62 message_id=drain-1793929066 handler_running=true
stop returned false
```

When terminal 2 prints `call 1` and then `stop returned true` on Ctrl-C, the
handler had already finished. When it prints no `call 1` at all, the publish
was a duplicate or came before the consumer existed.

Running the same `serve` command again delivers the message once more, with
`Mesh-Delivery-Attempt: 2` and the same `Mesh-Message-Id`.

## Cleaning Up

```sh
streams rm DEMO_CREATED
```

Stopping `nats-server` with Ctrl-C ends everything else. The next session's
`mktemp -d` starts with an empty store.
