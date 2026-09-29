# Public API

| Constant                        | Role                                                         |
|---------------------------------|--------------------------------------------------------------|
| `ServiceMesh::Target`, `ServiceMap`, `Message`, `Endpoint`, `Subscriber` | `Data` values from the `service_mesh` gem. `Message#payload` is always `Encoding::BINARY`. |
| `ServiceMeshNats::Client.new(config, service_map, logger: nil)` | `#request(message, opts = {})`, `#publish(message, opts = {})`, `#close`, `#connection`, `#service_map` |
| `ServiceMeshNats::Runtime.new(client, config, endpoints: [], subscribers: [], logger: Logger.new($stderr))` | `#client`, `#start`, `#stop(drain_seconds)`, `#running?`, `#service_map` |
| `ServiceMesh::KindMismatch`, `InvalidTarget`, `NoDeploymentGroup` | the specification's errors, from `service_mesh` |
| `ServiceMeshNats::BadConfig`, `Closed`, `AlreadyStarted`, `Stopped`, `HandlerError` | this transport's errors, listed under Transport errors in [What the NATS Runtime Decides](runtime-behavior.md) |

A `Client` owns one NATS connection. `new` opens it and returns the connected
client; a connection failure passes through from `new`. `request` and
`publish` raise `Closed` after `close`. `close` closes the connection, returns
`nil`, and is idempotent; a closed client is final. `connection` returns the
`NATS::IO::Client` the client owns and raises `Closed` after `close`;
`Runtime` subscribes through it. Connection errors that nats-pure reports
asynchronously go to `logger:` when one is given.

A `Runtime` is built from a `Client` the application constructed. `client`
returns that object in every state and `service_map` returns its map.

`Runtime#stop` returns `true` when every in-flight handler finished within
the drain and `false` when some were abandoned.
