# Public API

Every public constant and method in `ServiceMeshNats`.

| Name | Role |
|---|---|
| `ServiceMeshNats::Client.new(config, service_map, logger: nil)` | Opens one NATS connection and returns the connected client, as described under [The Client](transport-specific-implementation.md#the-client). |
| `ServiceMeshNats::Client` | Implements the contract's `Client`. Its methods are `#request(message, opts = {})`, `#publish(message, opts = {})`, `#close`, and `#service_map`. `#connection` returns the `NATS::IO::Client` it owns, which `Runtime` subscribes through. |
| `ServiceMeshNats::Runtime.new(client, config, endpoints: [], subscribers: [], logger: Logger.new($stderr))` | Builds a runtime that serves the endpoints and subscribers on the client's connection, as described under [The Runtime and Its Client](transport-specific-implementation.md#the-runtime-and-its-client). |
| `ServiceMeshNats::Runtime` | Implements the contract's `Runtime`. Its methods are `#start`, `#stop(drain)`, `#running?`, `#client`, and `#service_map`. |
| `URL_KEY`, `NAME_KEY`, `CONNECT_TIMEOUT_KEY`, `REQUEST_TIMEOUT_KEY`, `CONCURRENCY_KEY` | The configuration keys, described under [Configuration](transport-specific-implementation.md#configuration). `REQUEST_TIMEOUT_KEY` is also a per-call option. |
| `DEFAULT_URL`, `DEFAULT_CONNECT_TIMEOUT`, `DEFAULT_REQUEST_TIMEOUT` | The defaults for those keys. |
| `HANDLER_ERROR_HEADER` | `"Mesh-Handler-Error"`, the reply header the runtime sets when an endpoint handler fails. |
| `ServiceMeshNats::Error`, `BadConfig`, `Closed`, `AlreadyStarted`, `Stopped`, `HandlerError` | The errors this gem defines, listed under [Errors](transport-specific-implementation.md#errors). |
| `ServiceMeshNats::VERSION` | The gem's version. |

`ClientSettings`, `RuntimeSettings`, `Settings`, and `Subject` are shared
between `Client` and `Runtime`, and applications do not use them.
