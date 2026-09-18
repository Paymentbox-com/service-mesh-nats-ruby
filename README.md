# service-mesh-nats-ruby

A Ruby implementation of the Service Mesh API Specification over NATS,
packaged as the gem `service_mesh_nats`.

## Install

```ruby
# Gemfile
gem "service_mesh_nats"
```

Requires Ruby 3.3 or newer and a reachable NATS server.

## Usage

```ruby
require "service_mesh_nats"

echo    = ServiceMeshNats::Target.new(segments: %w[demo echo], kind: :route)
created = ServiceMeshNats::Target.new(segments: %w[demo created], kind: :topic)
map     = ServiceMeshNats::ServiceMap.new(targets: [echo, created])

config = {
  "url" => "nats://127.0.0.1:4222",
  "deployment_group" => "demo"
}

runtime = ServiceMeshNats::Runtime.new(config, map,
  endpoints: [
    ServiceMeshNats::Endpoint.new(target: echo, handler: ->(m) { ServiceMeshNats::Message.new(target: echo, payload: m.payload) })
  ],
  subscribers: [
    ServiceMeshNats::Subscriber.new(target: created, handler: ->(m) { puts "created: #{m.payload}" })
  ])
# Raises NoDeploymentGroup, BadConfig, KindMismatch, InvalidTarget, or DuplicateTarget.

runtime.start
at_exit { runtime.stop(10) }

client = runtime.client
reply = client.request(ServiceMeshNats::Message.new(target: echo, payload: "hi"))
client.publish(ServiceMeshNats::Message.new(target: created, payload: "order 42"))
```

A process that only calls builds `ServiceMeshNats::Client.new(config)` and
calls `close` when done.

## Public API

| constant                        | role                                                         |
|---------------------------------|--------------------------------------------------------------|
| `Target`, `ServiceMap`, `Message`, `Endpoint`, `Subscriber` | `Data` values from the specification. `Message#payload` is always `Encoding::BINARY`. |
| `Client.new(config)`            | `#request(message, opts = {})`, `#publish(message, opts = {})`, `#close` |
| `Runtime.new(config, service_map, endpoints:, subscribers:, logger:)` | `#client`, `#start`, `#stop(drain_seconds)`, `#running?`, `#service_map` |
| `KindMismatch`, `InvalidTarget`, `NoDeploymentGroup` | the specification's errors |
| `BadConfig`, `NotRunning`, `AlreadyStarted`, `Stopped`, `DuplicateTarget`, `HandlerError` | this runtime's errors |

`Runtime#stop` returns `true` when every in-flight handler finished within
the drain and `false` when some were abandoned.

## What the NATS runtime decides

The specification leaves these to each transport.

**Targets.** Segments join with `.` into a subject. A segment must be
non-empty and free of `.`, `*`, `>`, whitespace, and non-printable
characters. Targets are literal; no wildcards.

**Configuration.** All values are strings. Beyond `deployment_group` the
keys are:

| key               | default                  | meaning                                            |
|-------------------|--------------------------|----------------------------------------------------|
| `url`             | `nats://127.0.0.1:4222`  | server URL                                         |
| `name`            | none                     | connection name reported to the server             |
| `connect_timeout` | `5`                      | seconds to wait for the initial connection         |
| `request_timeout` | `30`                     | seconds a `request` waits for a reply; also accepted as a per-call option |
| `concurrency`     | processor count          | handler threads                                    |

Durations are seconds, whole or fractional, such as `"5"` or `"0.25"`. A
value that does not parse raises `BadConfig`. A `Logger` is passed to
`Runtime.new` as the `logger:` keyword and defaults to standard error.

**Metadata.** Message metadata rides as NATS headers, one value per key. The
runtime reads no message keys and writes `Mesh-Handler-Error` on a failed
reply. Binding metadata is read at construction. `consumer_group` is taken
from the `Endpoint` or `Subscriber` first, then from its `Target`.
`deployment_group` on a client-side `Target` is ignored.

**Delivery.** A consumer group is a NATS queue group. Every endpoint and
subscriber joins the deployment group unless `consumer_group` overrides it.
`none` gives a plain subscription, so every instance handles every message,
and for an endpoint every instance replies.

**Handler failure.** An endpoint handler that raises produces an empty reply
carrying the error message in the `Mesh-Handler-Error` header; the requester
gets `HandlerError` with that text. A failing subscriber handler is logged.
An endpoint handler that returns something other than a `Message` produces an
empty reply.

**Timeouts.** `request` raises `NATS::Timeout` when no reply arrives within
the request timeout, and `NATS::IO::NoRespondersError` when nothing serves
the target.

**Concurrency.** Handlers run on a fixed pool of `concurrency` threads.
Deliveries beyond that wait in the pool's queue.

**Lifecycle.** `start` connects, subscribes, and flushes so the server knows
every subscription before it returns. `stop(drain)` unsubscribes, waits up to
`drain` seconds for in-flight handlers, flushes, and closes. Handlers still
running at the end of the drain are left to finish on their own threads.
A runtime does not restart; `start` after `stop` raises `Stopped`.
`Runtime#client` shares the runtime's connection, its `close` is a no-op,
and it raises `NotRunning` outside the running window.

**Transport errors.** nats-pure errors pass through unchanged, including
connection failures such as `Errno::ECONNREFUSED`.

## Development

```
mise install
just install
just check      # lint, test, build
```

## Tests

```
just test
```

Integration specs start a real `nats-server` on a random loopback port per
example group. The binary is found on `PATH` or at `NATS_SERVER_BIN`.
