# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Runtime, :nats do
  let(:url) { nats_server_url }
  let(:echo) { ServiceMesh::Target.new(segments: %w[test echo], kind: :route) }
  let(:map) { ServiceMesh::ServiceMap.new }
  let(:quiet) { Logger.new(File::NULL) }
  let(:client) { ServiceMeshNats::Client.new("url" => url) }

  after { client.close }

  it "surfaces a connect failure and stays not running" do
    rt = described_class.new({"url" => "nats://127.0.0.1:1", "connect_timeout" => "0.2", "deployment_group" => "t"}, map, logger: quiet)
    expect { rt.start }.to raise_error(Errno::ECONNREFUSED)
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
    rt = described_class.new({"url" => url, "deployment_group" => "test", "concurrency" => "2"}, map,
      endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: handler)], logger: quiet)
    rt.start

    threads = 6.times.map { Thread.new { client.request(ServiceMesh::Message.new(target: echo)) } }
    threads.each(&:value)

    expect(peak.value).to eq(2)
  ensure
    rt&.stop(3)
  end
end
