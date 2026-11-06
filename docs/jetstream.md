# JetStream

This transport supports NATS JetStream for topic targets. A publish can wait
for a stream to confirm that it stored the message, and a subscriber can
consume from a JetStream consumer, so a message stays in the stream until a
handler has finished with it.

The `stream` setting turns JetStream on, and it is usually carried in the
target's metadata. One client and one runtime carry core NATS and JetStream
traffic together.

## Topic Targets Only

JetStream applies to topic targets only. A request waits for its reply, so
the caller already knows whether it was handled, and durability adds little.

A stream must never cover an endpoint's subject. Such a stream stores each
request and sends its publish ack to the requester. The ack races the
endpoint's reply and usually arrives first, so the caller takes the ack for
the reply while the handler is still running. When the endpoint is down, the
stream still answers, and the outage goes unnoticed.

These rules follow:

- `stream` in an `Endpoint`'s settings makes `Runtime.new` raise
  `BadConfig`.
- `stream` in a request's settings makes `request` raise `BadConfig`.
- `start` checks that no stream covers an endpoint subject, as described
  under [The Boot Check](#the-boot-check).

## Streams

Streams are created outside the runtime, before the processes that use them
start. `start` raises nats-pure's error when a subscriber's stream does not
exist.

Each JetStream topic target has its own stream, which holds exactly that
target's subject and no wildcards.

A named consumer group's consumer takes the group's name. A consumer name
only has to be unique within its stream, and each target has its own stream.
A group that subscribes to several targets therefore has one consumer on each
target's stream, all with the group's name, and they never collide.

`stream` is a property of the target, declared once in its metadata.
Publishers and subscribers do not override it, so every subscriber of a
subject consumes from the stream, and a publish whose stream is missing fails
right away.

The stream's retention decides how long it keeps a message:

| Retention | Behavior |
|---|---|
| `limits` | Keeps messages until the stream's age or size limits. |
| `interest` | Keeps a message until every consumer has acked it, and drops messages published while no consumer exists. |
| `workqueue` | Deletes a message once one consumer acks it. It allows one consumer per subject, so one named group or one `none` instance per target. The server enforces this, and its error passes through unchanged. |

## Options

All values are strings. Durations are seconds, whole or fractional, such as
`"30"` or `"0.5"`.

Each setting is read from the first of these that has it:

1. The per-call options of `request` or `publish`.
2. The `Endpoint`'s or `Subscriber`'s metadata.
3. The `Target`'s metadata.
4. The runtime's config, for the settings that have a runtime default.
5. The default in the table below.

The target's metadata holds the settings declared for the target, and the
first two override them for one call or one subscriber. `publish` reads
`stream` and `request_timeout`. A subscriber that has `stream` reads the
rest, and they are ignored everywhere else.

| Key | Read by | Default | Meaning | Sent over the connection |
|---|---|---|---|---|
| `stream` | `publish`, subscriber | none | The stream name. On `publish` it sends a JetStream publish and waits for the ack. On a subscriber it consumes from a consumer on the stream instead of a queue subscription. | Yes, to the server. On `publish` it goes in the `Nats-Expected-Stream` header. On a subscriber it names the stream in the runtime's consumer requests. |
| `request_timeout` | `publish` | the client's `request_timeout` | Bounds the wait for the ack. | No. It bounds how long the client waits for the server's ack. A JetStream publish carries no `Mesh-Timeout`. |
| `create_consumer` | subscriber | `"true"` | `"false"` leaves a named group's consumer to be created and updated outside the runtime. | No. It decides whether the runtime sends consumer settings at all. |
| `ack_policy` | subscriber | `"double"` | `"double"`, `"explicit"`, or `"none"`, described under [Ack Policies](#ack-policies). | Yes, to the server, in the consumer's settings: explicit for `double` and `explicit`, none for `none`. |
| `ack_wait` | subscriber | the server's 30 seconds | Seconds allowed before an unacked message is sent again. Ignored when `backoff` is set. | Yes, to the server, in the consumer's settings. |
| `max_deliver` | subscriber | the runtime config's `max_deliver`, which defaults to `5` | The most times one message is delivered. | Yes, to the server, in the consumer's settings. |
| `backoff` | subscriber | none | Comma-separated seconds, one for each delivery in turn, allowed before an unacked message is sent again. Deliveries past the last entry use the last entry. The server sets `ack_wait` to the first entry. `max_deliver` must exceed the number of entries. | Yes, to the server, in the consumer's settings. |
| `nak_delay` | subscriber | none | Seconds before a failed message is sent again. Unset sends it again right away. | Yes, to the server, with each nak. |
| `max_ack_pending` | subscriber | the server's 1000 | The most unacked messages the consumer allows at once. | Yes, to the server, in the consumer's settings. |
| `deliver_policy` | subscriber | `"all"` | `"all"` or `"new"`, where a newly created consumer starts in the stream. | Yes, to the server, in the consumer's settings. |
| `consumer_inactive_threshold` | subscriber | 7 days for a named group, 5 minutes for an ordered consumer, and the server's 5 seconds for other `none` consumers | Seconds the consumer outlives its last puller. | Yes, to the server, in the consumer's settings. |
| `pull_expiry` | subscriber | the runtime config's `pull_expiry`, or `30` | Seconds the server keeps a pull open while it has nothing to send. At least 1. | Yes, to the server, in each pull request. |
| `pull_heartbeat` | subscriber | the runtime config's `pull_heartbeat`, or half of `pull_expiry` up to 30 | Seconds between the server's heartbeats on an open pull that has nothing to send. Two missed heartbeats make the subscriber pull again. From 0.5 to 30, and no more than half of `pull_expiry`. An ordered consumer defaults to 5, or to half of `pull_expiry` when that is under 10. | Yes, to the server, in each pull request. |

The consumer's settings are sent only when the runtime creates or updates the
consumer, so with `create_consumer = "false"` none of them are sent. No
option is sent as a message header under its own key.

A value that does not parse, or is not one of its allowed values, makes
`Runtime.new` raise `BadConfig`. So does `create_consumer = "false"` on a
subscriber whose `consumer_group` is `none`. A `pull_expiry` or
`pull_heartbeat` outside its limits parses, and makes `start` raise
`BadConfig`.

`max_deliver` defaults to 5 and a named group's inactive threshold to 7 days.
The other consumer settings default to the server's. The server's
`max_deliver` is unlimited, which would send a message that never decodes
again forever.

The transport has no setting for the size of a pull. Pulls are sized by the
runtime's `concurrency`, as described under [Pulling](#pulling). A limit on
the bytes of a pull is not supported.

## Publishing

A plain `publish` to a subject a stream covers is stored, with no ack. When
the publish's settings have `stream`, `publish` sends a JetStream publish
with `Nats-Expected-Stream` set to the stream, so a subject that lands in a
different stream fails. It waits for the ack for up to `request_timeout`.

`publish` returns `nil` once the ack arrives. A publish the stream drops as a
duplicate also counts as success.

`Mesh-Message-Id` sets `Nats-Msg-Id`, so the stream drops a repeat of the
same message within its duplicate window. When a message carries both with
different values, `Mesh-Message-Id` wins.

An error carried in the ack, or raised by nats-pure, passes through
unchanged. What a missing stream raises depends on what else listens on the
subject:

| Stream | Core subscribers | Result |
|---|---|---|
| Yes | any | The ack, or the `NATS::JetStream::Error::APIError` carried in it |
| No | none | `NATS::JetStream::Error::NoStreamResponse` |
| No | one or more | `NATS::Timeout` once `request_timeout` passes |

## Subscribing

A subscriber with `stream` consumes from a pull consumer on that stream,
starting at `start`. Its resolved consumer group decides who shares a
consumer:

- All instances in a named group pull from one consumer named by the group,
  so each message goes to one instance of the group, and each group gets
  every message.
- With `none`, each instance pulls from its own consumer, which the runtime
  gives a new name at each `start`.

A restarted `none` instance creates a new consumer, which starts where
`deliver_policy` says, and does not resume its old one. An application that
wants a stable consumer per instance gives each instance its own consumer
group.

### Ordered Consumers

A `none` subscriber with `ack_policy = "none"` uses an ordered consumer. It
is kept in memory on the server with one replica, because the runtime can
always create it again.

The runtime checks that each delivery's consumer sequence is one more than
the last. A skipped sequence means a message was lost on the way. On a
skipped sequence, missed heartbeats, a deleted consumer, a pull with no
responders, or a reconnect, the runtime deletes the consumer and creates a
new one that starts at the stream sequence after the last message it handed
to a handler. No message in the stream is skipped, and none is delivered
twice.

Messages from any consumer, including an ordered one, are handed to handlers
that run concurrently, up to the runtime's `concurrency`. Handlers get no
ordering guarantee.

## Pulling

Each subscriber keeps up to `concurrency` messages owed by the server or
waiting for a handler. Its first pull asks for `concurrency` messages. When
fewer than half of `concurrency`, rounded up, are owed or waiting, or a pull
ends, it pulls for the difference.

Every handler in the runtime, for endpoints and subscribers alike, shares the
`concurrency` limit. A message that arrives while every handler is busy waits
for a free one. The server counts a waiting message as delivered and
unacked, so its `ack_wait` is already running, and it counts against
`max_ack_pending`.

Sizing pulls by `concurrency` keeps the number of waiting messages small, so
few deadlines run out while messages wait. In a named group, messages one
instance has not pulled stay available to the other instances.

When neither a message nor a heartbeat arrives for twice `pull_heartbeat`,
the subscriber logs a warning and pulls again. After a reconnect it pulls
again without a warning. While the connection is down it neither pulls nor
checks heartbeats.

## Ack Policies

`ack_policy` decides how the runtime settles each message:

- `double` acks each message and waits for the server to confirm the ack,
  for up to the client's `request_timeout`.
- `explicit` acks each message without waiting.
- `none` acks nothing, and nothing is sent again, so delivery is at most
  once.

With `none`, `ack_wait`, `backoff`, `max_deliver`, `nak_delay`,
`max_ack_pending`, and `Mesh-Deadline` do not apply.

`double` and `explicit` create the consumer with NATS's explicit ack policy.
With `create_consumer = "false"`, the server's ack policy decides whether
messages are acked, and `double` or `explicit` decides only whether the
runtime waits for the confirmation.

NATS's `all` ack policy is not offered. Acking one message under `all` acks
every earlier message the consumer delivered, across all of a group's
instances, including messages whose handlers are still running or about to
fail.

## Handler Results

A handler that returns is acked. A handler that raises is logged and nacked.
The nak sends the message again right away, or after `nak_delay` when it is
set. `max_deliver` bounds how many times one message is delivered, and
`backoff` sets the ack deadline of each delivery in turn.

Handlers must be idempotent, because a message can be handled more than once
after a missed deadline, a failed ack, or a nak at the end of a drain.
`ack_policy = "double"` confirms that each ack was recorded.

A message's ack deadline runs while it waits for a handler. A message that
waits past its deadline is sent again, and each of those deliveries counts
toward `max_deliver`.

Two log lines show this:

- `handler started after its ack deadline` warns that a handler started
  late, and `late_by` gives the seconds. It is logged at most once a minute
  for each subscriber, and `skipped` counts the late starts since the last
  warning that were not logged.
- `message will not be delivered again` reports a handler that failed on the
  message's last delivery under `max_deliver`.

## The Handler's Time Limit

A handler that runs past its ack deadline gets its message delivered again
while it is still running. The message metadata carries the deadline as
`Mesh-Deadline`, in RFC 3339 with nanoseconds, in UTC. A handler that checks
it can stop in time.

The deadline is the time the message reached the subscriber plus the
consumer's `ack_wait`, or plus the `backoff` entry for this delivery when the
consumer has `backoff`. The runtime reads these from the server's settings
for the consumer.

A consumer that does not ack gives no deadline.

## Delivery Metadata

Message headers reach handlers as metadata, including headers such as
`Nats-Msg-Id`. When a message carries `Nats-Msg-Id`, the transport also sets
`Mesh-Message-Id` to its value.

JetStream's delivery details are not headers. The transport copies them into
the metadata of each delivery:

| Key | Value |
|---|---|
| `Mesh-Delivery-Attempt` | Which delivery of this message this is, starting at 1. |
| `Mesh-Deadline` | The ack deadline, on deliveries from a consumer that acks. |
| `Mesh-Nats-Stream` | The stream name. |
| `Mesh-Nats-Consumer` | The consumer name. |
| `Mesh-Nats-Stream-Sequence` | The message's sequence in the stream. |
| `Mesh-Nats-Consumer-Sequence` | The delivery's sequence in the consumer. |
| `Mesh-Nats-Num-Pending` | How many messages are still waiting in the consumer. |
| `Mesh-Nats-Timestamp` | When the stream stored the message, in RFC 3339 with nanoseconds, in UTC. |

The `Mesh-Nats-` keys are also constants, such as
`ServiceMeshNats::STREAM_METADATA_KEY`. The prefix keeps them out of the
`Nats-` header space, which the server reads, in case a handler copies its
metadata onto a message it sends.

## Consumers Created by the Runtime

The runtime creates every consumer itself at `start`, because only the
running process knows each subscriber's resolved consumer group.

- A named group's consumer is created, or updated when it exists, with the
  subscriber's settings and the group's name.
- A consumer filters on the target's subject.
- A `none` subscriber's consumer gets a new name at each `start`.
- The runtime reads each consumer's settings back from the server, so
  `Mesh-Deadline` uses the server's values.

Because every `start` writes the settings, these effects follow:

- During a rolling deploy, old and new instances rewrite each other's
  consumer settings until the deploy finishes.
- Instances of a group whose settings differ keep rewriting the consumer
  while they all run, so every instance of a group needs the same consumer
  settings.
- A change to a setting the server does not allow to change after creation,
  such as `deliver_policy`, makes `start` raise the server's error. Applying
  the change means deleting the consumer by hand, which loses its position in
  the stream.
- Under `interest` retention, messages published before any consumer exists
  are dropped.

### Orphaned Consumers

A consumer whose subscriber is removed from code stays on the server, and
under `interest` retention it keeps every message it has not acked.

A named group's consumer expires once `consumer_inactive_threshold` passes
with no pulls, which defaults to 7 days. A running runtime pulls
continuously, so a consumer reaches the threshold only when every instance of
its group has stopped. A group that stays down longer than the threshold
loses its consumer, and its next `start` creates a new one under
`deliver_policy`. Under `interest` retention, the messages that no other
consumer still needed are gone by then.

### Consumers Managed Outside the Runtime

With `create_consumer = "false"`, the runtime creates and updates nothing for
a named group's consumer. At `start` it looks up the consumer named by the
group, and raises nats-pure's error when it does not exist. It reads the
consumer's settings from the server and ignores the subscriber's consumer
settings.

## A Missing Consumer

A consumer can disappear while the runtime runs. The server deletes an
acking `none` consumer after 5 seconds without pulls, so a disconnect longer
than that loses it. A consumer is also lost when someone deletes it or its
stream, or with the cluster node that held it when it has one replica.

The runtime does not create a missing consumer again, except an ordered
consumer, as described under [Ordered Consumers](#ordered-consumers). It
logs `consumer missing` at Error when it learns the consumer is gone, and
again every 5 seconds while the subscriber still receives nothing from it.

What follows depends on whether the connection was up when the consumer was
deleted:

- Deleted while connected, the server says so on the open pull, and the
  subscriber stops pulling. It receives nothing until the next `start`, even
  when the consumer is created again.
- Deleted while disconnected, each pull after the reconnect gets no
  responders, and the subscriber keeps pulling once a second. When a
  consumer with the same name is created again, by another instance's
  `start` or outside the runtime, the next pull gets messages and the log
  stops.

A pull that reaches the server while its consumer is being deleted can get no
answer at all. The subscriber then learns the consumer is gone only after two
missed heartbeats, which with the default settings is 30 seconds.

An acking `none` consumer has a name only the runtime knew, so nothing
creates it again, and its subscriber receives nothing until the next
`start`.

## Inspecting Streams and Consumers

The client and the runtime give access to JetStream state outside a handler.

| Name | Returns |
|---|---|
| `client.connection` | The client's `NATS::IO::Client`. |
| `client.jetstream` | The `NATS::JetStream` context the client publishes with. |
| `client.stream_info(target)` | The `StreamInfo` of the stream named by the target's `stream`, or `nil` when the target has none. |
| `runtime.receiver(target)` | The runtime's `ServiceMeshNats::Receiver` for the target's endpoint or subscriber while the runtime is running, or `nil` when it is not running or has neither for the target. |
| `receiver.target` | The receiver's target. |
| `receiver.consumer_group` | The resolved consumer group. |
| `receiver.stream_info` | The `StreamInfo` of the stream named by the receiver's settings, or `nil` when they have no `stream`, which is so for every endpoint. |
| `receiver.consumer_info` | The `ConsumerInfo` of the consumer the receiver pulls from at the time of the call, or `nil` when its settings have no `stream`. |

`client.stream_info` reads the target, so it names the stream a publish to
the target uses. A receiver's `stream_info` reads the receiver's settings, so
it names the stream the subscriber consumes.

After the client is closed, `connection`, `jetstream`, `stream_info`, and a
receiver's `stream_info` and `consumer_info` raise `Closed`. A lookup failure
raises nats-pure's error unchanged.

For a `none` consumer, `receiver.consumer_info` is the only way to reach the
consumer, because the runtime names it at each `start`. For an ordered
consumer, it reports the consumer the runtime is using at the time of the
call, including one created after a gap.

`ConsumerInfo` comes from nats-pure, which reports `ack_wait` and
`inactive_threshold` in whole seconds, rounded down.

A process that loads everything in a stream before it serves can wait on the
subscriber's consumer after `start`:

```ruby
receiver = runtime.receiver(target)
loop do
  info = receiver.consumer_info
  break if info.num_pending.zero? && info.num_ack_pending.zero?

  sleep 0.1
end
```

With `double` or `explicit` acks, no pending and no unacked messages means
every message up to that point was handled. With `ack_policy = "none"`,
nothing is acked, so the check shows only that every message was delivered,
while handlers may still be running.

`client.connection` is the connection the client and the runtime use.
Closing or draining it directly ends the client, and for a runtime built from
the client, its subscriptions and pull loops. `request` and `publish` then
raise nats-pure's error rather than `Closed`.

## The Boot Check

`start` looks up the stream of each endpoint subject. When a stream covers
one, `start` raises `StreamCoversRoute`, whose `subject` and `stream` name
them, and leaves the runtime and the client as they were.

Every runtime runs the check, including one with no JetStream subscriber. A
stream covering a route subject answers plain core requests, so the processes
most exposed are those that only serve endpoints.

A server without JetStream has no streams, so the check passes. A runtime
with no endpoints makes no lookups.

## Stopping

`stop(drain)` stops pulling and keeps handing the messages already received
to handlers while the drain runs. When every handler finishes in time, `stop`
returns `true`.

When the drain ends first, the runtime naks every message from an acking
consumer that is not yet settled, whether its handler had not started or was
still running, and `stop` returns `false`. Each delivery is settled once, by
its handler's result or by `stop`, whichever comes first. A message nacked
this way can be handled twice.

A handler still running keeps its thread until it returns or the process
exits, and its result is discarded. Killing a thread can interrupt it
anywhere, so the runtime does not. Handlers have `Mesh-Deadline` to stop on
their own.

The runtime logs a warning for each message it naks at the end of the drain:

```
service_mesh_nats: nacked a message at the end of the drain subject=orders.created stream=ORDERS consumer=billing stream_sequence=42 message_id=order-42 handler_running=true
```

`message_id` appears when the message has a `Mesh-Message-Id`.
`handler_running` is `true` when its handler was still running and `false`
when it had not started. When the nak fails, the line ends with `error=`.
