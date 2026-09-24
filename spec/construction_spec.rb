# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Runtime, "construction", :nats do
  let(:config) { {"deployment_group" => "billing"} }
  let(:route) { ServiceMesh::Target.new(segments: %w[a b], kind: :route) }
  let(:topic) { ServiceMesh::Target.new(segments: %w[a b], kind: :topic) }
  let(:ok) { ->(m) { m } }
  let(:map) { ServiceMesh::ServiceMap.new(targets: [route, topic]) }
  let(:client) { ServiceMeshNats::Client.new({"url" => nats_server_url}, map) }

  after { client.close }

  def endpoint(target, metadata: {})
    ServiceMesh::Endpoint.new(target: target, metadata: metadata, handler: ok)
  end

  def subscriber(target, metadata: {})
    ServiceMesh::Subscriber.new(target: target, metadata: metadata, handler: ok)
  end

  it "rejects a target the transport cannot carry" do
    bad = ServiceMesh::Target.new(segments: ["a.b"], kind: :route)
    expect { described_class.new(client, config, endpoints: [endpoint(bad)]) }.to raise_error(ServiceMesh::InvalidTarget)
  end

  it "rejects a handler that cannot be called" do
    ep = ServiceMesh::Endpoint.new(target: route, handler: :not_callable)
    expect { described_class.new(client, config, endpoints: [ep]) }.to raise_error(ArgumentError)
  end

  it "ignores connection keys in its config" do
    rt = described_class.new(client, config.merge("url" => "nats://127.0.0.1:1", "connect_timeout" => "later"))
    expect(rt.client).to equal(client)
  end

  it "exposes the client's service map" do
    expect(described_class.new(client, config).service_map).to equal(map)
  end

  it "resolves consumer groups for endpoints and subscribers" do
    none = {"consumer_group" => "none"}
    rt = described_class.new(client, config,
      endpoints: [endpoint(route, metadata: none)],
      subscribers: [subscriber(topic.with(segments: %w[c d]))])
    bindings = rt.instance_variable_get(:@bindings)
    expect(bindings.map(&:queue)).to eq([nil, "billing"])
  end
end
