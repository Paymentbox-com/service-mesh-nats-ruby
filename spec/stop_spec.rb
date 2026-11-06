# frozen_string_literal: true

require "stringio"

RSpec.describe ServiceMeshNats::Runtime, "stop", :nats do
  let(:map) { ServiceMesh::ServiceMap.new }
  let(:config) { nats_config("deployment_group" => "test") }
  let(:admin) { ServiceMeshNats::Client.new(config, map) }
  let(:log) { StringIO.new }

  after do
    (@runtimes || []).each { |runtime| runtime.stop(0) }
    admin.close
  end

  def stream_target(name)
    subject = "stop.#{name.downcase}"
    admin.jetstream.add_stream(name: name, subjects: [subject])
    ServiceMesh::Target.new(segments: subject.split("."), kind: :topic, metadata: {"stream" => name})
  end

  def start_runtime(runtime_config: config, endpoints: [], subscribers: [])
    runtime = described_class.new(ServiceMeshNats::Client.new(runtime_config, map), runtime_config,
      endpoints: endpoints, subscribers: subscribers, logger: Logger.new(log))
    runtime.start
    (@runtimes ||= []) << runtime
    runtime
  end

  def publish(target, payload)
    admin.publish(ServiceMesh::Message.new(target: target, payload: payload))
  end

  def nak_lines
    log.string.lines.grep(/nacked a message at the end of the drain/)
  end

  it "naks and logs a message whose handler outlasts the drain, and it is delivered again" do
    target = stream_target("OUTLAST")
    started = Queue.new
    release = Queue.new
    handler = lambda do |message|
      started << message.payload
      release.pop
    end
    runtime = start_runtime(subscribers: [ServiceMesh::Subscriber.new(target: target, consumer_group: "audit",
      handler: handler, metadata: {"ack_wait" => "30"})])
    admin.publish(ServiceMesh::Message.new(target: target, payload: "slow",
      metadata: {ServiceMesh::MESSAGE_ID_KEY => "m-1"}))
    started.pop(timeout: 3)

    expect(runtime.stop(0.3)).to be(false)
    release << true

    expect(nak_lines.length).to eq(1)
    expect(nak_lines.first).to include("service_mesh_nats: nacked a message at the end of the drain subject=stop.outlast " \
      "stream=OUTLAST consumer=audit stream_sequence=1 message_id=m-1 handler_running=true")

    seen = Queue.new
    start_runtime(subscribers: [ServiceMesh::Subscriber.new(target: target, consumer_group: "audit",
      handler: ->(message) { seen << message.metadata[ServiceMesh::DELIVERY_ATTEMPT_KEY] })])
    expect(seen.pop(timeout: 3)).to eq("2")
  end

  it "naks both the running message and the one waiting for the permit with concurrency 1" do
    target = stream_target("WAITING")
    started = Queue.new
    handler = lambda do |message|
      started << message.payload
      sleep 2
    end
    runtime = start_runtime(runtime_config: config.merge("concurrency" => "1"),
      subscribers: [ServiceMesh::Subscriber.new(target: target, consumer_group: "audit", handler: handler)])
    publish(target, "first")
    publish(target, "second")
    started.pop(timeout: 3)
    sleep 0.3

    expect(runtime.stop(0.3)).to be(false)

    expect(nak_lines.length).to eq(2)
    expect(nak_lines.join).to include("stream_sequence=1 handler_running=true")
    expect(nak_lines.join).to include("stream_sequence=2 handler_running=false")
  end

  it "settles a delivery once when its handler finishes after the drain" do
    target = stream_target("ONCE")
    started = Queue.new
    finished = Queue.new
    handler = lambda do |message|
      started << message.payload
      sleep 0.6
      finished << true
    end
    runtime = start_runtime(subscribers: [ServiceMesh::Subscriber.new(target: target, consumer_group: "audit",
      handler: handler, metadata: {"ack_wait" => "30"})])
    publish(target, "x")
    started.pop(timeout: 3)

    runtime.stop(0.1)
    finished.pop(timeout: 3)
    sleep 0.2

    # The nak leaves the message pending for redelivery, and the handler's
    # late result does not ack it.
    expect(nak_lines.length).to eq(1)
    expect(log.string).not_to include("ack failed")
    expect(admin.jetstream.consumer_info("ONCE", "audit").num_ack_pending).to eq(1)
  end

  it "finishes a drain that outlasts no handler, and naks nothing" do
    target = stream_target("QUICK")
    seen = Queue.new
    runtime = start_runtime(subscribers: [ServiceMesh::Subscriber.new(target: target, consumer_group: "audit",
      handler: ->(message) { seen << message.payload })])
    publish(target, "x")
    seen.pop(timeout: 3)

    expect(runtime.stop(2)).to be(true)
    expect(nak_lines).to eq([])
    expect(admin.jetstream.consumer_info("QUICK", "audit").num_ack_pending).to eq(0)
  end

  it "answers core requests waiting for a handler when stop begins" do
    echo = ServiceMesh::Target.new(segments: %w[stop echo], kind: :route)
    handler = lambda do |message|
      sleep 0.3
      message
    end
    runtime = start_runtime(runtime_config: config.merge("concurrency" => "1"),
      endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: handler)])
    replies = Queue.new
    threads = 3.times.map do |index|
      Thread.new do
        replies << admin.request(ServiceMesh::Message.new(target: echo, payload: index.to_s), "request_timeout" => "5").payload
      rescue => error
        replies << error
      end
    end
    sleep 0.1

    expect(runtime.stop(5)).to be(true)
    threads.each(&:join)

    expect(3.times.map { replies.pop }.sort).to eq(%w[0 1 2])
  end

  it "returns without waiting when the client was closed directly" do
    target = stream_target("CLOSED")
    handler = ->(_) { sleep 2 }
    client = ServiceMeshNats::Client.new(config, map)
    runtime = described_class.new(client, config, logger: Logger.new(log),
      subscribers: [ServiceMesh::Subscriber.new(target: target, consumer_group: "audit", handler: handler)])
    runtime.start
    client.close

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    runtime.stop(5)

    expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 1
  end
end
