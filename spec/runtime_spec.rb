# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Runtime, :nats do
  let(:echo) { ServiceMesh::Target.new(segments: %w[test echo], kind: :route) }
  let(:map) { ServiceMesh::ServiceMap.new }
  let(:quiet) { Logger.new(File::NULL) }
  let(:client) { ServiceMeshNats::Client.new(nats_config, map) }
  let(:config) { nats_config("deployment_group" => "test") }

  after { client.close }

  def new_client
    ServiceMeshNats::Client.new(nats_config, map)
  end

  def echo_runtime(given = new_client, extra = {})
    described_class.new(given, config.merge(extra),
      endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: ->(message) { message })], logger: quiet)
  end

  it "exposes the given client in every state" do
    given = new_client
    runtime = echo_runtime(given)
    expect(runtime.client).to equal(given)
    runtime.start
    expect(runtime.client).to equal(given)
    runtime.stop(3)
    expect(runtime.client).to equal(given)
  end

  it "raises Closed from start when the client is closed" do
    given = new_client
    given.close
    runtime = echo_runtime(given)
    expect { runtime.start }.to raise_error(ServiceMeshNats::Closed)
    expect(runtime.running?).to be(false)
  end

  it "leaves the runtime and the client as they were when subscribing fails" do
    given = new_client
    runtime = echo_runtime(given)
    allow(given.connection).to receive(:flush).and_raise(NATS::IO::Timeout)

    expect { runtime.start }.to raise_error(NATS::IO::Timeout)
    expect(runtime.running?).to be(false)
    expect(given.connection).to be_a(NATS::IO::Client)

    allow(given.connection).to receive(:flush).and_call_original
    runtime.start
    expect(runtime.running?).to be(true)
    runtime.stop(3)
  end

  it "closes the given client on stop" do
    given = new_client
    runtime = echo_runtime(given)
    runtime.start
    runtime.stop(3)
    expect { given.request(ServiceMesh::Message.new(target: echo)) }.to raise_error(ServiceMeshNats::Closed)
  end

  it "stops after its client was closed directly" do
    runtime = echo_runtime
    runtime.start
    runtime.client.close
    expect(runtime.stop(0)).to be(true)
    expect(runtime.running?).to be(false)
  end

  it "reports a non-Message endpoint return as HandlerError naming the class and keeps serving" do
    returns = [nil, ServiceMesh::Message.new(target: echo, payload: "next")]
    runtime = described_class.new(new_client, config,
      endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: ->(_) { returns.shift })], logger: quiet)
    runtime.start

    expect { client.request(ServiceMesh::Message.new(target: echo)) }
      .to raise_error(ServiceMeshNats::HandlerError) do |error|
        expect(error.text).to eq("endpoint test.echo handler returned NilClass, expected ServiceMesh::Message")
      end
    expect(client.request(ServiceMesh::Message.new(target: echo)).payload).to eq("next")
  ensure
    runtime&.stop(3)
  end

  it "runs at most `concurrency` handlers at once and reaches that bound" do
    in_flight = Concurrent::AtomicFixnum.new(0)
    peak = Concurrent::AtomicFixnum.new(0)
    handler = lambda do |message|
      running = in_flight.increment
      peak.update { |highest| [highest, running].max }
      sleep 0.05
      in_flight.decrement
      message
    end
    runtime = described_class.new(new_client, config.merge("concurrency" => "2"),
      endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: handler)], logger: quiet)
    runtime.start

    threads = 6.times.map { Thread.new { client.request(ServiceMesh::Message.new(target: echo)) } }
    threads.each(&:value)

    expect(peak.value).to eq(2)
  ensure
    runtime&.stop(3)
  end
end
