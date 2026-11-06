# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Runtime, "boot check", :nats do
  let(:map) { ServiceMesh::ServiceMap.new }
  let(:config) { nats_config("deployment_group" => "test") }
  let(:admin) { ServiceMeshNats::Client.new(config, map) }
  let(:echo) { ->(message) { message } }

  after do
    (@runtimes || []).each { |runtime| runtime.stop(1) }
    admin.close
  end

  def route(*segments)
    ServiceMesh::Target.new(segments: segments, kind: :route)
  end

  def endpoint(target)
    ServiceMesh::Endpoint.new(target: target, handler: echo)
  end

  def build(runtime_config: config, endpoints: [], subscribers: [])
    runtime = described_class.new(ServiceMeshNats::Client.new(runtime_config, map), runtime_config,
      endpoints: endpoints, subscribers: subscribers, logger: Logger.new(File::NULL))
    (@runtimes ||= []) << runtime
    runtime
  end

  def request(client, target)
    client.request(ServiceMesh::Message.new(target: target, payload: "ping"))
  end

  it "fails start when a stream covers an endpoint subject" do
    admin.jetstream.add_stream(name: "COVERED", subjects: ["boot.covered"])
    runtime = build(endpoints: [endpoint(route("boot", "covered"))])

    expect { runtime.start }.to raise_error(ServiceMeshNats::StreamCoversRoute) { |error|
      expect([error.subject, error.stream]).to eq(["boot.covered", "COVERED"])
      expect(error.message).to eq('a stream covers an endpoint subject: subject "boot.covered", stream "COVERED"')
    }
  end

  it "fails start when a stream's wildcard subject covers an endpoint subject" do
    admin.jetstream.add_stream(name: "WILDCARD", subjects: ["boot.wild.>"])
    runtime = build(endpoints: [endpoint(route("boot", "wild", "echo"))])

    expect { runtime.start }.to raise_error(ServiceMeshNats::StreamCoversRoute, /stream "WILDCARD"/)
  end

  it "subscribes nothing when the check fails" do
    admin.jetstream.add_stream(name: "BEFORE", subjects: ["boot.before"])
    runtime = build(endpoints: [endpoint(route("boot", "free")), endpoint(route("boot", "before"))])

    expect { runtime.start }.to raise_error(ServiceMeshNats::StreamCoversRoute)
    expect { request(admin, route("boot", "free")) }.to raise_error(NATS::IO::NoRespondersError)
  end

  it "starts when no stream covers an endpoint subject" do
    admin.jetstream.add_stream(name: "ELSEWHERE", subjects: ["boot.elsewhere"])
    build(endpoints: [endpoint(route("boot", "open"))]).start

    expect(request(admin, route("boot", "open")).payload).to eq("ping")
  end

  it "does not check a subscriber's subject" do
    admin.jetstream.add_stream(name: "EVENTS", subjects: ["boot.event"])
    topic = ServiceMesh::Target.new(segments: %w[boot event], kind: :topic)
    subscriber = ServiceMesh::Subscriber.new(target: topic, handler: ->(_) {})

    expect { build(subscribers: [subscriber]).start }.not_to raise_error
  end

  it "passes on a server without JetStream" do
    plain_config = config.merge("url" => nats_server_without_jetstream_url)
    build(runtime_config: plain_config, endpoints: [endpoint(route("boot", "plain"))]).start
    plain = ServiceMeshNats::Client.new(plain_config, map)

    expect(request(plain, route("boot", "plain")).payload).to eq("ping")
  ensure
    plain&.close
  end
end
