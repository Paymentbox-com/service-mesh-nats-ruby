# service-mesh-nats-ruby

`service_mesh_nats` is the Ruby implementation of the
[Service Mesh API Specification](https://github.com/Paymentbox-com/service-mesh-api)
over NATS. It implements the Ruby contract in the `service_mesh` gem, from
[service-mesh-ruby](https://github.com/Paymentbox-com/service-mesh-ruby), on
top of the `nats-pure` client.

The `ServiceMeshNats` module provides:
* `Client`, which implements the contract's `Client` over one NATS connection
* `Runtime`, which implements the contract's `Runtime` on a `Client`'s connection
* the configuration keys it reads
* the errors it defines

The contract types it works with, such as `ServiceMesh::Target`,
`ServiceMesh::Message`, `ServiceMesh::Endpoint`, and `ServiceMesh::Subscriber`,
come from the `service_mesh` gem.

## Install

```ruby
# Gemfile
gem "service_mesh_nats"
```

Requires Ruby 3.3 or newer and a reachable NATS server. The gem depends on
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

client = ServiceMeshNats::Client.new(config, map)
# Connects. Raises BadConfig or the connection failure.

runtime = ServiceMeshNats::Runtime.new(client, config,
  endpoints: [
    ServiceMesh::Endpoint.new(target: echo, handler: ->(m) { ServiceMesh::Message.new(target: echo, payload: m.payload) })
  ],
  subscribers: [
    ServiceMesh::Subscriber.new(target: created, handler: ->(m) { puts "created: #{m.payload}" })
  ])
# Raises NoDeploymentGroup, BadConfig, KindMismatch, or InvalidTarget, and
# ArgumentError for a handler that does not respond to call.

runtime.start
at_exit { runtime.stop(10) }

reply = client.request(ServiceMesh::Message.new(target: echo, payload: "hi"))
client.publish(ServiceMesh::Message.new(target: created, payload: "order 42"))
```

The client is the runtime's connection. `runtime.client` returns it in every
state, and `runtime.stop` closes it. A process that only requests and publishes
builds a client itself and calls `close` when done. `runtime.service_map` and
`client.service_map` return the map the client was built with.

## Documentation

- [Examples](docs/examples.md): a server process, a call-only client process, and consumer groups
- [Public API](docs/public-api.md): every public constant and method in `ServiceMeshNats`
- [Transport Specific Implementation](docs/transport-specific-implementation.md): subjects, configuration, metadata, delivery, handler failure, timeouts, concurrency, lifecycle, and errors
- [Development](docs/development.md): the recipes and the tests
