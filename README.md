# service-mesh-nats-ruby

The NATS transport for the
[Service Mesh API Specification](https://github.com/Paymentbox-com/service-mesh-api)
in Ruby, packaged as the gem `service_mesh_nats`. The specification is the
authority for everything this gem does. The contract types and errors come
from [service-mesh-ruby](https://github.com/Paymentbox-com/service-mesh-ruby),
gem `service_mesh`, and this gem passes its conformance suite. The Go
counterpart is
[service-mesh-nats-go](https://github.com/Paymentbox-com/service-mesh-nats-go).
The [gRPC Service Mesh API](https://github.com/Paymentbox-com/grpc-service-mesh-api)
is the protocol layer that generates code served over this transport from
protobuf definitions, through its Ruby library
[grpc-service-mesh-ruby](https://github.com/Paymentbox-com/grpc-service-mesh-ruby).

## Install

```ruby
# Gemfile
gem "service_mesh_nats"
```

Requires Ruby 3.3 or newer and a reachable NATS server. Depends on
`service_mesh` and `nats-pure`.

## Usage

```ruby
require "service_mesh_nats"

echo    = ServiceMesh::Target.new(segments: %w[demo echo], kind: :route)
created = ServiceMesh::Target.new(segments: %w[demo created], kind: :topic)
map     = ServiceMesh::ServiceMap.new(targets: [echo, created])

config = {
  "url" => "nats://127.0.0.1:4222",
  "deployment_group" => "demo"
}

runtime = ServiceMeshNats::Runtime.new(config, map,
  endpoints: [
    ServiceMesh::Endpoint.new(target: echo, handler: ->(m) { ServiceMesh::Message.new(target: echo, payload: m.payload) })
  ],
  subscribers: [
    ServiceMesh::Subscriber.new(target: created, handler: ->(m) { puts "created: #{m.payload}" })
  ])
# Raises NoDeploymentGroup, BadConfig, KindMismatch, InvalidTarget, or DuplicateTarget.

runtime.start
at_exit { runtime.stop(10) }

client = runtime.client
reply = client.request(ServiceMesh::Message.new(target: echo, payload: "hi"))
client.publish(ServiceMesh::Message.new(target: created, payload: "order 42"))
```

A process that only calls builds `ServiceMeshNats::Client.new(config, map)`
and calls `close` when done. `runtime.service_map`,
`runtime.client.service_map`, and a standalone client's `service_map` return
the map each was built with; the transport does not validate targets against
it.

## Examples

Each snippet below runs as written against a local `nats-server`. The
`echo`, `created`, and `map` values are the ones declared under Usage.

### A server process

Serves one endpoint and one subscriber until SIGINT or SIGTERM, then drains
for up to ten seconds.

```ruby
runtime = ServiceMeshNats::Runtime.new(
  {"url" => ENV.fetch("NATS_URL", "nats://127.0.0.1:4222"), "deployment_group" => "demo"}, map,
  endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: ->(m) {
    ServiceMesh::Message.new(target: echo, payload: m.payload)
  })],
  subscribers: [ServiceMesh::Subscriber.new(target: created, handler: ->(m) {
    puts "created: #{m.payload}"
  })]
)
runtime.start

stop = Queue.new
%w[INT TERM].each { |sig| Signal.trap(sig) { stop << sig } }
stop.pop

runtime.stop(10) or warn "some handlers were abandoned" # drain up to 10 seconds
```

### A call-only client process

Makes one request with metadata and a per-call timeout, rescues each
outcome, then publishes an event.

```ruby
client = ServiceMeshNats::Client.new({"url" => ENV.fetch("NATS_URL", "nats://127.0.0.1:4222")}, map)

begin
  reply = client.request(
    ServiceMesh::Message.new(target: echo, metadata: {"Request-Id" => "1"}, payload: "hello"),
    {"request_timeout" => "2"}
  )
  puts "#{reply.payload} #{reply.metadata}"
rescue ServiceMesh::KindMismatch          # a topic target given to request
rescue NATS::IO::NoRespondersError        # nothing serves demo.echo
rescue NATS::Timeout                      # no reply within request_timeout
rescue ServiceMeshNats::HandlerError => e # the handler raised: e.text
end

client.publish(ServiceMesh::Message.new(target: created, payload: "order 42"))
client.close
```

### Consumer groups

Two deployments on one topic each handle every event once. A subscriber
with `consumer_group` set to `none` handles every event on every instance.

```ruby
on_created = ->(m) { puts "created: #{m.payload}" }
sub = ServiceMesh::Subscriber.new(target: created, handler: on_created)

# billing and audit each run this subscriber under their own deployment_group,
# so every event is handled once per deployment.
billing = ServiceMeshNats::Runtime.new({"url" => url, "deployment_group" => "billing"}, map, subscribers: [sub])
audit   = ServiceMeshNats::Runtime.new({"url" => url, "deployment_group" => "audit"}, map, subscribers: [sub])

# Every instance of a deployment handles every event: no group at all.
broadcast = ServiceMesh::Subscriber.new(
  target: created,
  metadata: {ServiceMesh::CONSUMER_GROUP_KEY => ServiceMesh::CONSUMER_GROUP_NONE},
  handler: on_created
)
cache = ServiceMeshNats::Runtime.new({"url" => url, "deployment_group" => "cache"}, map, subscribers: [broadcast])

[billing, audit, cache].each(&:start)
# One publish to created now produces three "created" lines: billing, audit, cache.
```

## Public API

| constant                        | role                                                         |
|---------------------------------|--------------------------------------------------------------|
| `ServiceMesh::Target`, `ServiceMap`, `Message`, `Endpoint`, `Subscriber` | `Data` values from the `service_mesh` gem. `Message#payload` is always `Encoding::BINARY`. |
| `ServiceMeshNats::Client.new(config, service_map)` | `#request(message, opts = {})`, `#publish(message, opts = {})`, `#close`, `#service_map` |
| `ServiceMeshNats::Runtime.new(config, service_map, endpoints:, subscribers:, logger:)` | `#client`, `#start`, `#stop(drain_seconds)`, `#running?`, `#service_map` |
| `ServiceMesh::KindMismatch`, `InvalidTarget`, `NoDeploymentGroup` | the specification's errors, from `service_mesh` |
| `ServiceMeshNats::BadConfig`, `NotRunning`, `AlreadyStarted`, `Stopped`, `DuplicateTarget`, `HandlerError` | this transport's errors |

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
`spec/conformance_spec.rb` runs the `service_mesh` gem's shared examples
against this transport; the other specs cover what is NATS-specific.
