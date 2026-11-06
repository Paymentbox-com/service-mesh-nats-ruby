# Transport Specific Implementation

The [Service Mesh API
Specification](https://github.com/Paymentbox-com/service-mesh-api) leaves some
behavior to each transport, such as how a `Target` becomes an address, what
configuration it reads, and how delivery and failures work. This page describes
what this transport does in each of those places.

## Targets

A target's segments join with `.` into a NATS subject. A segment must be
non-empty and free of `.`, `*`, `>`, whitespace, and non-printable characters.
Targets are literal and hold no wildcards.

## Configuration

All values are strings. `Client.new` reads the connection keys, and
`Runtime.new` reads `deployment_group`, which is required, and the runtime
keys. Each constructor ignores the other's keys, so one Hash can be given to
both.

| Key | Read by | Default | Meaning | Sent over the connection |
|---|---|---|---|---|
| `url` | `Client.new` | `nats://127.0.0.1:4222` | The server URL. An empty value takes the default. | No. It is where the client connects. |
| `name` | `Client.new` | none | The connection name reported to the server. | Yes, to the server, when the client connects. |
| `connect_timeout` | `Client.new` | `5` | The bound on the initial connection, in seconds. | No. |
| `request_timeout` | `Client.new` | `30` | How long `request` waits for a reply, and `publish` for a JetStream ack, in seconds. It is also read from per-call options and target metadata. | On `request`, yes, to the serving process, as the `Mesh-Timeout` header in whole milliseconds. On a JetStream `publish`, no. |
| `deployment_group` | `Runtime.new` | required | The consumer group that endpoints and subscribers join. | Yes, to the server, as the queue group of each subscription, or as the consumer name of a JetStream subscriber. |
| `concurrency` | `Runtime.new` | processor count | The most handlers that run at once, across every endpoint and subscriber. | It sizes each JetStream pull request. Otherwise no. |
| `max_deliver` | `Runtime.new` | `5` | The default `max_deliver` of JetStream subscribers. | Yes, to the server, in the settings of each consumer the runtime creates. |
| `pull_expiry` | `Runtime.new` | `30` | The default `pull_expiry` of JetStream subscribers, in seconds. | Yes, to the server, in each pull request. |
| `pull_heartbeat` | `Runtime.new` | half of `pull_expiry`, up to `30` | The default `pull_heartbeat` of JetStream subscribers, in seconds. | Yes, to the server, in each pull request. |

Durations are seconds, whole or fractional, such as `"5"` or `"0.25"`. A value
that does not parse raises `BadConfig`. A `Logger` is passed to `Runtime.new`
as the `logger:` keyword, and defaults to standard error. `Client.new` takes
the same keyword with no default.

The JetStream settings of targets, subscribers, and calls are described under
[Options](jetstream.md#options) in the JetStream page.

## Settings

A setting can be given in more than one place. Each is read from the first of
these that has it:

1. The per-call options of `request` or `publish`.
2. The `Endpoint`'s or `Subscriber`'s metadata.
3. The `Target`'s metadata.
4. The client's or runtime's config.
5. The default.

A setting is read only where it applies. `request` and `publish` read the
per-call options and the target, and a runtime reads an endpoint's or
subscriber's metadata and its target.

## Metadata

Message metadata travels as NATS headers, one value per key. These keys are
reserved:

| Key | Set by | Meaning |
|---|---|---|
| `Mesh-Timeout` | `request` | The request timeout, in whole milliseconds. |
| `Mesh-Deadline` | The runtime | When the handler's time is up, in RFC 3339 with nanoseconds, in UTC. An endpoint gets it from `Mesh-Timeout` and the time the request arrived. A JetStream subscriber gets it from its consumer's ack deadline. |
| `Mesh-Message-Id` | The caller | The message's ID. It and `Nats-Msg-Id` are one field. On a message sent, `Mesh-Message-Id` wins and also sets `Nats-Msg-Id`. On a message received, `Nats-Msg-Id` wins and also sets `Mesh-Message-Id`. |
| `Mesh-Delivery-Attempt` | The runtime | Which delivery of a JetStream message this is, starting at 1. |
| `Mesh-Handler-Error` | The runtime | The error message of an endpoint handler that failed, on its reply. |

A JetStream delivery also carries the `Mesh-Nats-` keys, described under
[Delivery Metadata](jetstream.md#delivery-metadata).

## Delivery

An `Endpoint`'s or `Subscriber`'s `consumer_group` field decides who shares
its messages. An empty or missing field takes the runtime's
`deployment_group`. The runtime reads the field when `Runtime.new` runs.

Without JetStream, a consumer group is a NATS queue group, so each message goes
to one instance of the group, and each group gets every message. `none` gives a
plain subscription, so every instance handles every message, and for an
endpoint every instance replies.

Two endpoints or subscribers on one subject are two NATS subscriptions, with
whatever delivery NATS gives them.

A subscriber whose settings have `stream` consumes from a JetStream consumer
instead, as described in [JetStream](jetstream.md).

## Handler Failure

An endpoint handler that raises produces an empty reply carrying the error
message in the `Mesh-Handler-Error` header. The requester receives a
`HandlerError` whose `#text` is that message.

An endpoint handler that returns something other than a `Message` fails the
same way. The runtime logs it, and the requester's `HandlerError` names the
endpoint and the class the handler returned.

A subscriber handler that raises is logged. A JetStream subscriber's message is
also nacked, as described under [Handler Results](jetstream.md#handler-results).

## Timeouts

`request` raises `NATS::Timeout` when no reply arrives within the request
timeout, and `NATS::IO::NoRespondersError` when nothing serves the target.

## Concurrency

At most `concurrency` handlers run at once, across every endpoint and
subscriber. A core message waits on its subscription for a free handler, and a
JetStream message waits in its subscriber's pull loop. Handlers run on a fixed
pool of `concurrency` threads.

## Lifecycle

### The Client

A `Client` owns one NATS connection. `Client.new` opens it and returns the
connected client, and a connection failure passes through from `Client.new`.
`close` closes the connection, returns `nil`, and is idempotent. A closed
client is final, and `request`, `publish`, `connection`, `jetstream`, and
`stream_info` on it raise `Closed`.

`connection` returns the `NATS::IO::Client` the client owns, and `jetstream`
the JetStream context it publishes with. Connection errors that nats-pure
reports asynchronously go to `logger:` when one is given.

### The Runtime and Its Client

A `Runtime` is built from a `Client` the application constructed, and that
client is the runtime's connection. `Runtime.new` does not touch the
connection.

`Runtime#client` returns the client in every state, and the client serves
requests before `start` as well as during it. `Runtime#service_map` is the
client's.

### Starting

`start` first checks that no stream covers an endpoint subject, as described
under [The Boot Check](jetstream.md#the-boot-check). It then subscribes each
endpoint and subscriber through `client.connection`, creates or looks up each
JetStream subscriber's consumer, and flushes, with a budget of five seconds, so
the server knows every subscription before `start` returns.

`start` on a closed client raises `Closed` and leaves the runtime not running.
A failure while starting undoes what was opened, leaves the runtime and the
client as they were, and passes through, so `start` can be called again.

### Stopping

`stop(drain)` drains every subscription and JetStream subscriber, waits up to
`drain` seconds for handlers, flushes when every handler finished, and closes
the client. Draining stops new deliveries and hands the messages already
received to their handlers. `stop` returns `true` when every handler finished
and `false` when the drain ended first. A negative `drain` raises
`ArgumentError`.

When the drain ends first, core messages still waiting for a handler are
dropped, and JetStream messages not yet settled are nacked, as described under
[Stopping](jetstream.md#stopping) in the JetStream page.

A handler still running when the drain ends keeps running after `stop`
returns, until it returns or the process exits, and its result is discarded.
The runtime does not kill its thread, because killing a thread can interrupt it
anywhere. Handlers have `Mesh-Deadline` to stop on their own.

`stop` on a runtime that is not running returns `true` and leaves the client as
it is. Closing the client directly ends the runtime's connection, and a `stop`
that follows skips the flush and returns the drain result.

A runtime does not restart. `start` after `stop` raises `Stopped`.

## Logging

The runtime logs through the `Logger` given to `Runtime.new`. Each line is
`service_mesh_nats:`, then the message, then its fields as `key=value` pairs.
A field with no value is left out.

| Level | Message | Logged when | Fields |
|---|---|---|---|
| Error | `endpoint handler failed` | An endpoint handler raises or returns something other than a `Message`. | `subject`, `error` |
| Error | `subscriber handler failed` | A subscriber handler raises. | `subject`, `error`, and `stream` and `delivery_attempt` for JetStream |
| Error | `reply failed` | Publishing an endpoint's reply fails. | `subject`, `error` |
| Error | `message will not be delivered again` | A JetStream handler fails on the message's last delivery under `max_deliver`, after the nak. | `subject`, `stream`, `consumer`, `stream_sequence`, `delivery_attempt`, `message_id` when present, `max_deliver` |
| Error | `consumer missing` | A JetStream subscriber's consumer no longer exists, and every 5 seconds after that while the subscriber still receives nothing from it, as described under [A Missing Consumer](jetstream.md#a-missing-consumer). Not logged for an ordered consumer. | `subject`, `stream`, `consumer`, and `error` when a lookup fails |
| Error | `ack failed` | Acking a JetStream message after its handler returned fails. | `subject`, `stream`, `error` |
| Error | `nak failed` | Nacking a JetStream message after its handler failed fails. | `subject`, `stream`, `error` |
| Error | `pull loop stopped` | A JetStream subscriber's pull loop ends on an unexpected error, so the subscriber receives nothing until the next `start`. | `subject`, `stream`, `error` |
| Warn | `consume error` | Neither a message nor a heartbeat arrives on a JetStream subscriber's open pull for twice `pull_heartbeat`, and the subscriber pulls again. | `subject`, `stream`, `error` |
| Warn | `handler started after its ack deadline` | A JetStream handler starts after its delivery's ack deadline, as described under [Handler Results](jetstream.md#handler-results). Logged at most once a minute for each subscriber. | `subject`, `stream`, `consumer`, `stream_sequence`, `delivery_attempt`, `message_id` when present, `late_by`, `skipped` |
| Warn | `message has no JetStream metadata` | A JetStream delivery's metadata cannot be read, so the handler gets the message without the delivery metadata keys. | `subject`, `error` |
| Warn | `nacked a message at the end of the drain` | `stop`'s drain ends while a JetStream message is not settled. | `subject`, `stream`, `consumer`, `stream_sequence`, `message_id` when present, `handler_running`, and `error` when the nak fails |
| Warn | `replacing the ordered consumer` | An ordered consumer is replaced, as described under [Ordered Consumers](jetstream.md#ordered-consumers). | `subject`, `stream`, `consumer`, `reason` |
| Warn | `deleting the ordered consumer failed` | Deleting an ordered consumer that is being replaced fails. The server ends it after its inactive threshold. | `subject`, `stream`, `consumer`, `error` |
| Warn | `creating the ordered consumer failed` | Creating an ordered consumer's replacement fails. It is tried again after a second. | `subject`, `stream`, `error` |
| Warn | `pull got an unexpected status` | The server answers a pull with a status the runtime does not expect. The subscriber pulls again. | `subject`, `stream`, `status`, `description` |
| Warn | `flush during stop failed` | The flush after a finished drain fails. | `error` |

The client logs `connection error` at Warn, with the field `error`, for each
connection error nats-pure reports asynchronously, to the `logger:` given to
`Client.new`.

## Errors

nats-pure errors pass through unchanged, including connection failures such as
`Errno::ECONNREFUSED`. The three contract errors, `ServiceMesh::KindMismatch`,
`ServiceMesh::InvalidTarget`, and `ServiceMesh::NoDeploymentGroup`, come from
`service_mesh`.

These are the errors this gem defines, all under `ServiceMeshNats::Error`.

| Error | Raised when |
|---|---|
| `BadConfig` | A configuration value, setting, or per-call option does not parse, `stream` is set on an endpoint or a request, or a pull setting is outside its limits at `start`. |
| `Closed` | `request`, `publish`, `connection`, `jetstream`, or `stream_info` is called on a client after `close`, which for a runtime's client is after `stop`, or `start` is called on a runtime whose client is closed. |
| `AlreadyStarted` | `start` is called on a running runtime. |
| `Stopped` | `start` is called on a runtime after `stop`. |
| `StreamCoversRoute` | `start` finds a stream covering an endpoint subject. `#subject` and `#stream` name them. |
| `HandlerError` | The serving endpoint handler raised or returned something other than a `Message`. `#text` is its message. |
