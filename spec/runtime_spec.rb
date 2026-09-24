# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Runtime, :nats do
  let(:url) { nats_server_url }
  let(:echo) { ServiceMesh::Target.new(segments: %w[test echo], kind: :route) }
  let(:map) { ServiceMesh::ServiceMap.new }
  let(:quiet) { Logger.new(File::NULL) }
  let(:client) { ServiceMeshNats::Client.new({"url" => url}, map) }
  let(:config) { {"url" => url, "deployment_group" => "test"} }

  after { client.close }

  def echo_runtime(extra = {})
    described_class.new(config.merge(extra), map,
      endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: ->(m) { m })], logger: quiet)
  end

  it "surfaces a connect failure and stays not running" do
    rt = described_class.new({"url" => "nats://127.0.0.1:1", "connect_timeout" => "0.2", "deployment_group" => "t"}, map, logger: quiet)
    expect { rt.start }.to raise_error(Errno::ECONNREFUSED)
    expect(rt.running?).to be(false)
  end

  it "keeps one client across start" do
    rt = echo_runtime
    before = rt.client
    rt.start
    expect(rt.client).to equal(before)
  ensure
    rt&.stop(3)
  end

  it "closes its client on stop" do
    rt = echo_runtime
    rt.start
    rt.stop(3)
    expect { rt.client.request(ServiceMesh::Message.new(target: echo)) }.to raise_error(ServiceMeshNats::Closed)
  end

  it "stops after its client was closed directly" do
    rt = echo_runtime
    rt.start
    rt.client.close
    expect(rt.stop(0)).to be(true)
    expect(rt.running?).to be(false)
  end

  it "runs at most `concurrency` handlers at once and reaches that bound" do
    in_flight = Concurrent::AtomicFixnum.new(0)
    peak = Concurrent::AtomicFixnum.new(0)
    handler = lambda do |m|
      n = in_flight.increment
      peak.update { |p| [p, n].max }
      sleep 0.05
      in_flight.decrement
      m
    end
    rt = described_class.new(config.merge("concurrency" => "2"), map,
      endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: handler)], logger: quiet)
    rt.start

    threads = 6.times.map { Thread.new { client.request(ServiceMesh::Message.new(target: echo)) } }
    threads.each(&:value)

    expect(peak.value).to eq(2)
  ensure
    rt&.stop(3)
  end
end
