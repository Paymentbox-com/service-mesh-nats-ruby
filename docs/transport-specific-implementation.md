# What the NATS Runtime Decides

The specification leaves these to each transport.

**Targets.** Segments join with `.` into a subject. A segment must be
non-empty and free of `.`, `*`, `>`, whitespace, and non-printable
characters. Targets are literal; no wildcards.

**Configuration.** All values are strings. `Client.new` reads the connection
keys and `Runtime.new` reads `deployment_group` and `concurrency`; each
ignores the other's keys, so one hash can be given to both.

| Key               | Read by   | Default                  | Meaning                                            |
|-------------------|-----------|--------------------------|----------------------------------------------------|
| `url`             | `Client`  | `nats://127.0.0.1:4222`  | server URL                                         |
| `name`            | `Client`  | none                     | connection name reported to the server             |
| `connect_timeout` | `Client`  | `5`                      | seconds to wait for the initial connection         |
| `request_timeout` | `Client`  | `30`                     | seconds a `request` waits for a reply; also accepted as a per-call option |
| `deployment_group`| `Runtime` | required                 | the specification's deployment group               |
| `concurrency`     | `Runtime` | processor count          | handler threads                                    |

Durations are seconds, whole or fractional, such as `"5"` or `"0.25"`. A
value that does not parse raises `BadConfig`. A `Logger` is passed to
`Runtime.new` as the `logger:` keyword and defaults to standard error.
`Client.new` takes the same keyword with no default.

**Metadata.** Message metadata rides as NATS headers, one value per key. The
runtime reads no message keys and writes `Mesh-Handler-Error` on a failed
reply. Endpoint and subscriber metadata is read at construction. `consumer_group` is taken
from the `Endpoint` or `Subscriber` first, then from its `Target`.
`deployment_group` on a client-side `Target` is ignored.

**Delivery.** A consumer group is a NATS queue group. Every endpoint and
subscriber joins the deployment group unless `consumer_group` overrides it.
`none` gives a plain subscription, so every instance handles every message,
and for an endpoint every instance replies. Two endpoints or subscribers on
one subject are two NATS subscriptions, with whatever delivery NATS gives them.

**Handler failure.** An endpoint handler that raises produces an empty reply
carrying the error message in the `Mesh-Handler-Error` header; the requester
gets `HandlerError` with that text. A failing subscriber handler is logged.
An endpoint handler that returns something other than a `Message` is a handler
failure: the runtime logs it and the requester gets `HandlerError` whose text
names the endpoint and the class the handler returned.

**Timeouts.** `request` raises `NATS::Timeout` when no reply arrives within
the request timeout, and `NATS::IO::NoRespondersError` when nothing serves
the target.

**Concurrency.** Handlers run on a fixed pool of `concurrency` threads.
Deliveries beyond that wait in the pool's queue.

**Lifecycle.** The client given to `Runtime.new` owns the runtime's
connection. `Runtime#client` returns that object in every state, and the
client serves requests before `start` as well as during it. `Runtime.new`
does not touch the connection. `start` subscribes through
`client.connection` and flushes, with a budget of five seconds, so the server
knows every subscription before it returns; `start` on a closed client raises
`Closed` and leaves the runtime not running. `stop(drain)` unsubscribes,
waits up to `drain` seconds for in-flight handlers, flushes when every
handler finished, and closes the client. A negative `drain` raises
`ArgumentError`. Handlers still running at the end of the drain are left to finish on
their own threads. The client's `request` and `publish` raise `Closed` after
`stop`. `stop` on a runtime that has not started returns `true` and leaves
the client open. Closing the client directly ends the runtime's connection; a
`stop` that follows skips the flush and returns the drain result. A runtime
does not restart; `start` after `stop` raises `Stopped`. A failure while
subscribing closes the client and marks the runtime stopped.

**Transport errors.** nats-pure errors pass through unchanged, including
connection failures such as `Errno::ECONNREFUSED`. This transport's own
errors, all under `ServiceMeshNats::Error`:

| Error             | Raised when                                                                 |
|-------------------|-----------------------------------------------------------------------------|
| `BadConfig`       | a configuration value or per-call option does not parse                     |
| `Closed`          | `request`, `publish`, or `connection` on a client after `close`; `start` on a runtime whose client is closed |
| `AlreadyStarted`  | `start` on a running runtime                                                |
| `Stopped`         | `start` on a runtime after `stop`                                           |
| `HandlerError`    | the serving endpoint handler raised; `#text` is its message                 |
