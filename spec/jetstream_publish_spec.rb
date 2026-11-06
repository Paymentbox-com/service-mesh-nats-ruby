# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Client, "JetStream publish", :nats do
  let(:map) { ServiceMesh::ServiceMap.new }
  let(:config) { nats_config }
  let(:client) { described_class.new(config, map) }

  after do
    client.close
    (@raw || []).each(&:close)
  end

  def topic(*segments, metadata: {})
    ServiceMesh::Target.new(segments: segments, kind: :topic, metadata: metadata)
  end

  def add_stream(name, subject)
    client.jetstream.add_stream(name: name, subjects: [subject])
  end

  def stored(name)
    client.jetstream.stream_info(name).state.messages
  end

  it "stores a publish to an existing stream" do
    add_stream("STORED", "js.stored")

    client.publish(ServiceMesh::Message.new(target: topic("js", "stored"), payload: "x"), "stream" => "STORED")

    expect(stored("STORED")).to eq(1)
  end

  it "raises nats-pure's no-stream error for a subject no stream takes" do
    expect { client.publish(ServiceMesh::Message.new(target: topic("js", "nowhere")), "stream" => "MISSING") }
      .to raise_error(NATS::JetStream::Error::NoStreamResponse)
  end

  it "raises the stream's error for a subject that lands in a different stream" do
    add_stream("ACTUAL", "js.actual")

    expect { client.publish(ServiceMesh::Message.new(target: topic("js", "actual")), "stream" => "EXPECTED") }
      .to raise_error(NATS::JetStream::Error::APIError, /stream/)
  end

  it "stores one message for two publishes with the same Mesh-Message-Id" do
    add_stream("DEDUP", "js.dedup")
    message = ServiceMesh::Message.new(target: topic("js", "dedup"), metadata: {ServiceMesh::MESSAGE_ID_KEY => "id-1"})

    2.times { client.publish(message, "stream" => "DEDUP") }

    expect(stored("DEDUP")).to eq(1)
  end

  it "publishes through JetStream when the target sets stream" do
    add_stream("TARGETED", "js.targeted")
    target = topic("js", "targeted", metadata: {"stream" => "OTHER"})

    expect { client.publish(ServiceMesh::Message.new(target: target)) }
      .to raise_error(NATS::JetStream::Error::APIError, /stream/)
  end

  it "bounds the wait for the ack by the publish's request_timeout" do
    raw = NATS::IO::Client.new
    raw.connect(servers: [nats_server_url])
    (@raw ||= []) << raw
    raw.subscribe("js.unacked") { |_| }
    raw.flush

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    expect { client.publish(ServiceMesh::Message.new(target: topic("js", "unacked")), "stream" => "S", "request_timeout" => "0.3") }
      .to raise_error(NATS::Timeout)
    expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be_between(0.25, 1)
  end
end
