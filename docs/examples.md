# Examples

Each snippet below runs as written against a local `nats-server`. The
`echo`, `created`, and `map` values are the values declared under [Usage](../README.md#usage).

## A Server Process

Serves one endpoint and one subscriber until SIGINT or SIGTERM, then drains
for up to ten seconds.

```ruby
config = {"url" => ENV.fetch("NATS_URL", "nats://127.0.0.1:4222"), "deployment_group" => "demo"}
runtime = ServiceMeshNats::Runtime.new(
  ServiceMeshNats::Client.new(config, map), config,
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

## A Call-Only Client Process

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

## Consumer Groups

Two deployments on one topic each handle every event once. A subscriber
with `consumer_group` set to `none` handles every event on every instance.

```ruby
url = ENV.fetch("NATS_URL", "nats://127.0.0.1:4222")
on_created = ->(m) { puts "created: #{m.payload}" }
sub = ServiceMesh::Subscriber.new(target: created, handler: on_created)

# billing and audit each run this subscriber under their own deployment_group,
# so every event is handled once per deployment.
serve = ->(group, subscribers) do
  config = {"url" => url, "deployment_group" => group}
  ServiceMeshNats::Runtime.new(ServiceMeshNats::Client.new(config, map), config, subscribers: subscribers)
end
billing = serve.call("billing", [sub])
audit   = serve.call("audit", [sub])

# With no consumer group, every instance of a deployment handles every event.
broadcast = ServiceMesh::Subscriber.new(
  target: created,
  metadata: {ServiceMesh::CONSUMER_GROUP_KEY => ServiceMesh::CONSUMER_GROUP_NONE},
  handler: on_created
)
cache = serve.call("cache", [broadcast])

[billing, audit, cache].each(&:start)
# One publish to created now produces three "created" lines: billing, audit, cache.
```
