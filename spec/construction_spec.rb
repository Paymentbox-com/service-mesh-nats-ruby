# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Runtime, "construction" do
  let(:config) { {"deployment_group" => "billing"} }
  let(:route) { ServiceMeshNats::Target.new(segments: %w[a b], kind: :route) }
  let(:topic) { ServiceMeshNats::Target.new(segments: %w[a b], kind: :topic) }
  let(:ok) { ->(m) { m } }
  let(:map) { ServiceMeshNats::ServiceMap.new }

  def endpoint(target, metadata: {})
    ServiceMeshNats::Endpoint.new(target: target, metadata: metadata, handler: ok)
  end

  def subscriber(target, metadata: {})
    ServiceMeshNats::Subscriber.new(target: target, metadata: metadata, handler: ok)
  end

  it "rejects a missing deployment group" do
    expect { described_class.new({}, map) }.to raise_error(ServiceMeshNats::NoDeploymentGroup)
  end

  it "rejects an endpoint on a topic" do
    expect { described_class.new(config, map, endpoints: [endpoint(topic)]) }.to raise_error(ServiceMeshNats::KindMismatch)
  end

  it "rejects a subscriber on a route" do
    expect { described_class.new(config, map, subscribers: [subscriber(route)]) }.to raise_error(ServiceMeshNats::KindMismatch)
  end

  it "rejects a target the transport cannot carry" do
    bad = ServiceMeshNats::Target.new(segments: ["a.b"], kind: :route)
    expect { described_class.new(config, map, endpoints: [endpoint(bad)]) }.to raise_error(ServiceMeshNats::InvalidTarget)
  end

  it "rejects two endpoints on one subject" do
    expect { described_class.new(config, map, endpoints: [endpoint(route), endpoint(route)]) }.to raise_error(ServiceMeshNats::DuplicateTarget)
  end

  it "rejects an endpoint and a subscriber on one subject" do
    expect { described_class.new(config, map, endpoints: [endpoint(route)], subscribers: [subscriber(topic)]) }
      .to raise_error(ServiceMeshNats::DuplicateTarget)
  end

  it "rejects a handler that cannot be called" do
    ep = ServiceMeshNats::Endpoint.new(target: route, handler: :not_callable)
    expect { described_class.new(config, map, endpoints: [ep]) }.to raise_error(ArgumentError)
  end

  it "keeps the service map" do
    sm = ServiceMeshNats::ServiceMap.new(targets: [route, topic])
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

  it "is not running before start" do
    expect(described_class.new(config, map).running?).to be(false)
  end
end

RSpec.describe ServiceMeshNats::Target do
  it "rejects an unknown kind" do
    expect { described_class.new(segments: %w[a], kind: :queue) }.to raise_error(ArgumentError)
  end

  it "compares channels by segments and kind only" do
    a = described_class.new(segments: %w[x], kind: :route, metadata: {"k" => "1"})
    b = described_class.new(segments: %w[x], kind: :route)
    c = described_class.new(segments: %w[x], kind: :topic)
    expect(a.same_channel?(b)).to be(true)
    expect(a.same_channel?(c)).to be(false)
  end
end

RSpec.describe ServiceMeshNats::Message do
  it "forces the payload to binary" do
    m = described_class.new(target: ServiceMeshNats::Target.new(segments: %w[x], kind: :route), payload: "héllo")
    expect(m.payload.encoding).to eq(Encoding::BINARY)
    expect(m.payload.bytesize).to eq(6)
  end

  it "defaults to an empty payload and metadata" do
    m = described_class.new(target: ServiceMeshNats::Target.new(segments: %w[x], kind: :route))
    expect([m.payload, m.metadata]).to eq(["", {}])
  end
end
