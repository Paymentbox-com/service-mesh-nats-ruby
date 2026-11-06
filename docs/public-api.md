# Public API

Every public constant and method in `ServiceMeshNats`.

| Name | Role |
|---|---|
| `ServiceMeshNats::Client.new(config, service_map, logger: nil)` | Opens one NATS connection and returns the connected client, as described under [The Client](transport-specific-implementation.md#the-client). |
| `ServiceMeshNats::Client` | Implements the contract's `Client`. Its methods are `#request(message, opts = {})`, `#publish(message, opts = {})`, `#close`, and `#service_map`. |
| `Client#connection`, `Client#jetstream` | The `NATS::IO::Client` the client owns, and the `NATS::JetStream` context it publishes with. Both raise `Closed` after `close`. |
| `Client#stream_info(target)` | The `StreamInfo` of the stream named by the target's `stream`, or `nil` when it has none, as described under [Inspecting Streams and Consumers](jetstream.md#inspecting-streams-and-consumers). |
| `Client#request_timeout` | The client's configured `request_timeout`, in seconds. |
| `ServiceMeshNats::Runtime.new(client, config, endpoints: [], subscribers: [], logger: Logger.new($stderr))` | Builds a runtime that serves the endpoints and subscribers on the client's connection, as described under [The Runtime and Its Client](transport-specific-implementation.md#the-runtime-and-its-client). |
| `ServiceMeshNats::Runtime` | Implements the contract's `Runtime`. Its methods are `#start`, `#stop(drain)`, `#running?`, `#client`, and `#service_map`. |
| `Runtime#receiver(target)` | The runtime's `Receiver` for the target's endpoint or subscriber while it is running, or `nil`. |
| `ServiceMeshNats::Receiver` | An endpoint or subscriber the runtime is running. Its methods are `#target`, `#consumer_group`, `#stream_info`, and `#consumer_info`, described under [Inspecting Streams and Consumers](jetstream.md#inspecting-streams-and-consumers). |
| `URL_KEY`, `NAME_KEY`, `CONNECT_TIMEOUT_KEY`, `REQUEST_TIMEOUT_KEY`, `CONCURRENCY_KEY`, `MAX_DELIVER_KEY`, `PULL_EXPIRY_KEY`, `PULL_HEARTBEAT_KEY` | The configuration keys, described under [Configuration](transport-specific-implementation.md#configuration). `REQUEST_TIMEOUT_KEY` is also a per-call option and a target setting. The last three are also subscriber settings. |
| `STREAM_KEY`, `CREATE_CONSUMER_KEY`, `ACK_POLICY_KEY`, `ACK_WAIT_KEY`, `BACKOFF_KEY`, `NAK_DELAY_KEY`, `MAX_ACK_PENDING_KEY`, `DELIVER_POLICY_KEY`, `CONSUMER_INACTIVE_THRESHOLD_KEY` | The JetStream settings, described under [Options](jetstream.md#options). |
| `DEFAULT_URL`, `DEFAULT_CONNECT_TIMEOUT`, `DEFAULT_REQUEST_TIMEOUT`, `DEFAULT_MAX_DELIVER`, `DEFAULT_PULL_EXPIRY`, `NAMED_INACTIVE_THRESHOLD`, `ORDERED_INACTIVE_THRESHOLD` | The defaults for those keys. The inactive thresholds are in seconds. |
| `STREAM_METADATA_KEY`, `CONSUMER_METADATA_KEY`, `STREAM_SEQUENCE_METADATA_KEY`, `CONSUMER_SEQUENCE_METADATA_KEY`, `NUM_PENDING_METADATA_KEY`, `TIMESTAMP_METADATA_KEY` | The `Mesh-Nats-` keys of a JetStream delivery's metadata, described under [Delivery Metadata](jetstream.md#delivery-metadata). |
| `ServiceMeshNats::Error`, `BadConfig`, `Closed`, `AlreadyStarted`, `Stopped`, `StreamCoversRoute`, `HandlerError` | The errors this gem defines, listed under [Errors](transport-specific-implementation.md#errors). |
| `ServiceMeshNats::VERSION` | The gem's version. |

The reserved metadata keys, such as `Mesh-Handler-Error` and `Mesh-Deadline`,
are constants of the `service_mesh` gem, such as
`ServiceMesh::HANDLER_ERROR_KEY`.

`ClientSettings`, `RuntimeSettings`, `JetStreamSettings`, `Config`, `Target`,
and `Runtime::Subscription` are shared between `Client` and `Runtime`, and
applications do not use them.
