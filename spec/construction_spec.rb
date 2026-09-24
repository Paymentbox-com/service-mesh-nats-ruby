# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Runtime, "construction" do
  let(:config) { {"deployment_group" => "billing"} }
  let(:route) { ServiceMesh::Target.new(segments: %w[a b], kind: :route) }
  let(:topic) { ServiceMesh::Target.new(segments: %w[a b], kind: :topic) }
  let(:ok) { ->(m) { m } }
  let(:map) { ServiceMesh::ServiceMap.new }

  def endpoint(target, metadata: {})
    ServiceMesh::Endpoint.new(target: target, metadata: metadata, handler: ok)
  end

  def subscriber(target, metadata: {})
    ServiceMesh::Subscriber.new(target: target, metadata: metadata, handler: ok)
  end

  it "rejects a target the transport cannot carry" do
    bad = ServiceMesh::Target.new(segments: ["a.b"], kind: :route)
    expect { described_class.new(config, map, endpoints: [endpoint(bad)]) }.to raise_error(ServiceMesh::InvalidTarget)
  end

  it "rejects a handler that cannot be called" do
    ep = ServiceMesh::Endpoint.new(target: route, handler: :not_callable)
    expect { described_class.new(config, map, endpoints: [ep]) }.to raise_error(ArgumentError)
  end

  it "keeps the service map" do
    sm = ServiceMesh::ServiceMap.new(targets: [route, topic])
    expect(described_class.new(config, sm).service_map).to equal(sm)
  end

  it "resolves consumer groups for endpoints and subscribers" do
    none = {"consumer_group" => "none"}
    rt = described_class.new(config, map,
      endpoints: [endpoint(route, metadata: none)],
      subscribers: [subscriber(topic.with(segments: %w[c d]))])
    bindings = rt.instance_variable_get(:@bindings)
    expect(bindings.map(&:queue)).to eq([nil, "billing"])
  end
end
