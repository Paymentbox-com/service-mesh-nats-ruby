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
`Runtime.new` reads `deployment_group`, which is required, and `concurrency`.
Each constructor ignores the other's keys, so one Hash can be given to both.

| Key | Read by | Default | Meaning |
|---|---|---|---|
| `url` | `Client.new` | `nats://127.0.0.1:4222` | The server URL. An empty value takes the default. |
| `name` | `Client.new` | none | The connection name reported to the server. |
| `connect_timeout` | `Client.new` | `5` | The bound on the initial connection, in seconds. |
| `request_timeout` | `Client.new` | `30` | How long `request` waits for a reply, in seconds. It is also accepted as a per-call option. |
| `deployment_group` | `Runtime.new` | required | The queue group that endpoints and subscribers join. |
| `concurrency` | `Runtime.new` | processor count | The number of handler threads. |

Durations are seconds, whole or fractional, such as `"5"` or `"0.25"`. A value
that does not parse raises `BadConfig`. A `Logger` is passed to `Runtime.new`
as the `logger:` keyword, and defaults to standard error. `Client.new` takes
the same keyword with no default.

## Metadata

Message metadata travels as NATS headers, one value per key. The runtime reads
no message keys, and writes `Mesh-Handler-Error` on a failed reply.

Endpoint and subscriber metadata is read when `Runtime.new` runs.
`consumer_group` is taken from the `Endpoint` or `Subscriber` first, then from
its `Target`. `deployment_group` on a client-side `Target` is ignored.

## Delivery

A consumer group is a NATS queue group. Every endpoint and subscriber joins the
deployment group unless `consumer_group` overrides it.

`none` gives a plain subscription, so every instance handles every message, and
for an endpoint every instance replies.

Two endpoints or subscribers on one subject are two NATS subscriptions, with
whatever delivery NATS gives them.

## Handler Failure

An endpoint handler that raises produces an empty reply carrying the error
message in the `Mesh-Handler-Error` header. The requester receives a
`HandlerError` whose `#text` is that message.

An endpoint handler that returns something other than a `Message` fails the
same way. The runtime logs it, and the requester's `HandlerError` names the
endpoint and the class the handler returned.

A subscriber handler that raises is logged.

## Timeouts

`request` raises `NATS::Timeout` when no reply arrives within the request
timeout, and `NATS::IO::NoRespondersError` when nothing serves the target.

## Concurrency

Handlers run on a fixed pool of `concurrency` threads. Deliveries beyond that
wait in the pool's queue.

## Lifecycle

### The Client

A `Client` owns one NATS connection. `Client.new` opens it and returns the
connected client, and a connection failure passes through from `Client.new`.
`close` closes the connection, returns `nil`, and is idempotent. A closed
client is final, and `request`, `publish`, and `connection` on it raise
`Closed`.

`connection` returns the `NATS::IO::Client` the client owns, which `Runtime`
subscribes through. Connection errors that nats-pure reports asynchronously go
to `logger:` when one is given.

### The Runtime and Its Client

A `Runtime` is built from a `Client` the application constructed, and that
client is the runtime's connection. `Runtime.new` does not touch the
connection.

`Runtime#client` returns the client in every state, and the client serves
requests before `start` as well as during it. `Runtime#service_map` is the
client's.

### Starting

`start` subscribes through `client.connection` and flushes, with a budget of
five seconds, so the server knows every subscription before `start` returns.

`start` on a closed client raises `Closed` and leaves the runtime not running.
A subscribe or flush failure unsubscribes what was subscribed, leaves the
runtime and the client as they were, and passes through.

### Stopping

`stop(drain)` unsubscribes, waits up to `drain` seconds for in-flight handlers,
flushes when every handler finished, and closes the client. It returns `true`
when every handler finished and `false` when some were abandoned. Handlers
still running at the end of the drain are left to finish on their own threads.
A negative `drain` raises `ArgumentError`.

`stop` on a runtime that is not running returns `true` and leaves the client as
it is. Closing the client directly ends the runtime's connection, and a `stop`
that follows skips the flush and returns the drain result.

A runtime does not restart. `start` after `stop` raises `Stopped`.

## Errors

nats-pure errors pass through unchanged, including connection failures such as
`Errno::ECONNREFUSED`. The three contract errors, `ServiceMesh::KindMismatch`,
`ServiceMesh::InvalidTarget`, and `ServiceMesh::NoDeploymentGroup`, come from
`service_mesh`.

These are the errors this gem defines, all under `ServiceMeshNats::Error`.

| Error | Raised when |
|---|---|
| `BadConfig` | A configuration value or per-call option does not parse. |
| `Closed` | `request`, `publish`, or `connection` is called on a client after `close`, which for a runtime's client is after `stop`, or `start` is called on a runtime whose client is closed. |
| `AlreadyStarted` | `start` is called on a running runtime. |
| `Stopped` | `start` is called on a runtime after `stop`. |
| `HandlerError` | The serving endpoint handler raised or returned something other than a `Message`. `#text` is its message. |
