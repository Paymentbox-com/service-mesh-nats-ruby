# frozen_string_literal: true

require "time"

RSpec.describe "reserved metadata keys", :nats do
  let(:echo) { ServiceMesh::Target.new(segments: %w[test echo], kind: :route) }
  let(:event) { ServiceMesh::Target.new(segments: %w[test event], kind: :topic) }
  let(:map) { ServiceMesh::ServiceMap.new }
  let(:config) { nats_config }
  let(:quiet) { Logger.new(File::NULL) }

  after do
    (@runtimes || []).each { |runtime| runtime.stop(3) }
    (@closers || []).each(&:close)
  end

  def serve(endpoints: [], subscribers: [])
    client = ServiceMeshNats::Client.new(config, map)
    runtime = ServiceMeshNats::Runtime.new(client, config.merge("deployment_group" => "test"),
      endpoints: endpoints, subscribers: subscribers, logger: quiet)
    runtime.start
    (@runtimes ||= []) << runtime
    runtime
  end

  def client(client_config = config)
    new_client = ServiceMeshNats::Client.new(client_config, map)
    (@closers ||= []) << new_client
    new_client
  end

  def raw_connection
    connection = NATS::IO::Client.new
    connection.connect(servers: [nats_server_url])
    (@closers ||= []) << connection
    connection
  end

  it "gives a request's handler Mesh-Timeout and a Mesh-Deadline from it" do
    seen = Queue.new
    serve(endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: lambda { |message|
      seen << message.metadata
      ServiceMesh::Message.new(target: echo)
    })])

    sent = Time.now
    client(config.merge("request_timeout" => "2")).request(ServiceMesh::Message.new(target: echo))
    metadata = seen.pop(timeout: 3)

    expect(metadata[ServiceMesh::TIMEOUT_KEY]).to eq("2000")
    deadline = Time.iso8601(metadata[ServiceMesh::DEADLINE_KEY])
    expect(deadline).to be_within(0.1).of(sent + 2)
  end

  it "gives a subscriber no Mesh-Deadline for a published Mesh-Timeout" do
    seen = Queue.new
    serve(subscribers: [ServiceMesh::Subscriber.new(target: event, handler: ->(message) { seen << message.metadata })])

    client.publish(ServiceMesh::Message.new(target: event, metadata: {ServiceMesh::TIMEOUT_KEY => "2000"}))

    expect(seen.pop(timeout: 3)).not_to have_key(ServiceMesh::DEADLINE_KEY)
  end

  it "copies a request's Mesh-Message-Id to Nats-Msg-Id" do
    headers = Queue.new
    connection = raw_connection
    connection.subscribe("test.echo") do |msg|
      headers << msg.header
      msg.respond("")
    end
    connection.flush

    client.request(ServiceMesh::Message.new(target: echo, metadata: {ServiceMesh::MESSAGE_ID_KEY => "id-1"}))

    expect(headers.pop(timeout: 3)["Nats-Msg-Id"]).to eq("id-1")
  end

  it "copies a publish's Mesh-Message-Id to Nats-Msg-Id" do
    headers = Queue.new
    connection = raw_connection
    connection.subscribe("test.event") { |msg| headers << msg.header }
    connection.flush

    client.publish(ServiceMesh::Message.new(target: event, metadata: {ServiceMesh::MESSAGE_ID_KEY => "id-1"}))

    expect(headers.pop(timeout: 3)["Nats-Msg-Id"]).to eq("id-1")
  end

  it "gives a handler Nats-Msg-Id as Mesh-Message-Id over a different Mesh-Message-Id" do
    seen = Queue.new
    serve(subscribers: [ServiceMesh::Subscriber.new(target: event, handler: ->(message) { seen << message.metadata })])

    raw_connection.publish_msg(NATS::Msg.new(subject: "test.event", data: "",
      header: {"Nats-Msg-Id" => "native-id", ServiceMesh::MESSAGE_ID_KEY => "stale-id"}))

    expect(seen.pop(timeout: 3)[ServiceMesh::MESSAGE_ID_KEY]).to eq("native-id")
  end

  it "delivers a publish carrying both IDs with Mesh-Message-Id's value in both" do
    seen = Queue.new
    serve(subscribers: [ServiceMesh::Subscriber.new(target: event, handler: ->(message) { seen << message.metadata })])

    client.publish(ServiceMesh::Message.new(target: event,
      metadata: {ServiceMesh::MESSAGE_ID_KEY => "mesh-id", "Nats-Msg-Id" => "native-id"}))

    metadata = seen.pop(timeout: 3)
    expect([metadata[ServiceMesh::MESSAGE_ID_KEY], metadata["Nats-Msg-Id"]]).to eq(%w[mesh-id mesh-id])
  end
end
