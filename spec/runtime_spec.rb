# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Runtime, :nats do
  let(:url) { nats_server_url }
  let(:echo) { ServiceMesh::Target.new(segments: %w[test echo], kind: :route) }
  let(:map) { ServiceMesh::ServiceMap.new }
  let(:quiet) { Logger.new(File::NULL) }
  let(:client) { ServiceMeshNats::Client.new({"url" => url}, map) }
  let(:config) { {"url" => url, "deployment_group" => "test"} }

  after { client.close }

  def new_client
    ServiceMeshNats::Client.new({"url" => url}, map)
  end

  def echo_runtime(given = new_client, extra = {})
    described_class.new(given, config.merge(extra),
      endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: ->(m) { m })], logger: quiet)
  end

  it "exposes the given client in every state" do
    given = new_client
    rt = echo_runtime(given)
    expect(rt.client).to equal(given)
    rt.start
    expect(rt.client).to equal(given)
    rt.stop(3)
    expect(rt.client).to equal(given)
  end

  it "raises Closed from start when the client is closed" do
    given = new_client
    given.close
    rt = echo_runtime(given)
    expect { rt.start }.to raise_error(ServiceMeshNats::Closed)
    expect(rt.running?).to be(false)
  end

  it "closes the given client on stop" do
    given = new_client
    rt = echo_runtime(given)
    rt.start
    rt.stop(3)
    expect { given.request(ServiceMesh::Message.new(target: echo)) }.to raise_error(ServiceMeshNats::Closed)
  end

  it "stops after its client was closed directly" do
    rt = echo_runtime
    rt.start
    rt.client.close
    expect(rt.stop(0)).to be(true)
    expect(rt.running?).to be(false)
  end

  it "reports a non-Message endpoint return as HandlerError naming the class and keeps serving" do
    returns = [nil, ServiceMesh::Message.new(target: echo, payload: "next")]
    rt = described_class.new(new_client, config,
      endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: ->(_) { returns.shift })], logger: quiet)
    rt.start

    expect { client.request(ServiceMesh::Message.new(target: echo)) }
      .to raise_error(ServiceMeshNats::HandlerError) do |e|
        expect(e.text).to eq("endpoint test.echo handler returned NilClass, expected ServiceMesh::Message")
      end
    expect(client.request(ServiceMesh::Message.new(target: echo)).payload).to eq("next")
  ensure
    rt&.stop(3)
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
    rt = described_class.new(new_client, config.merge("concurrency" => "2"),
      endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: handler)], logger: quiet)
    rt.start

    threads = 6.times.map { Thread.new { client.request(ServiceMesh::Message.new(target: echo)) } }
    threads.each(&:value)

    expect(peak.value).to eq(2)
  ensure
    rt&.stop(3)
  end
end
