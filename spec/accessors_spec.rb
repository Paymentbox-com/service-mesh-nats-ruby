# frozen_string_literal: true

RSpec.describe "accessors", :nats do
  let(:map) { ServiceMesh::ServiceMap.new }
  let(:config) { nats_config("deployment_group" => "test") }
  let(:client) { ServiceMeshNats::Client.new(config, map) }
  let(:echo) { ServiceMesh::Target.new(segments: %w[access echo], kind: :route) }

  after do
    (@runtimes || []).each { |runtime| runtime.stop(0) }
    client.close
  end

  def topic(name, stream: nil)
    ServiceMesh::Target.new(segments: ["access", name], kind: :topic, metadata: stream ? {"stream" => stream} : {})
  end

  def start_runtime(endpoints: [], subscribers: [])
    runtime = ServiceMeshNats::Runtime.new(ServiceMeshNats::Client.new(config, map), config,
      endpoints: endpoints, subscribers: subscribers, logger: Logger.new(File::NULL))
    runtime.start
    (@runtimes ||= []) << runtime
    runtime
  end

  describe "Client#stream_info" do
    it "returns the stream named by the target's stream" do
      client.jetstream.add_stream(name: "ACCESS", subjects: ["access.event"])

      expect(client.stream_info(topic("event", stream: "ACCESS")).config.name).to eq("ACCESS")
    end

    it "returns nil for a target without stream" do
      expect(client.stream_info(echo)).to be_nil
    end

    it "raises Closed after close" do
      client.close

      expect { client.stream_info(topic("event", stream: "ACCESS")) }.to raise_error(ServiceMeshNats::Closed)
    end

    it "raises nats-pure's error for a missing stream" do
      expect { client.stream_info(topic("event", stream: "NOSUCH")) }.to raise_error(NATS::JetStream::Error::NotFound)
    end
  end

  describe "Runtime#receiver" do
    it "returns an endpoint's receiver with its target and consumer group" do
      runtime = start_runtime(endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: ->(message) { message })])

      receiver = runtime.receiver(echo)

      expect([receiver.target, receiver.consumer_group]).to eq([echo, "test"])
      expect([receiver.stream_info, receiver.consumer_info]).to eq([nil, nil])
    end

    it "reports consumer group none for a receiver with none" do
      target = topic("none")
      runtime = start_runtime(subscribers: [ServiceMesh::Subscriber.new(target: target, consumer_group: "none",
        handler: ->(_) {})])

      expect(runtime.receiver(target).consumer_group).to eq(ServiceMesh::CONSUMER_GROUP_NONE)
    end

    it "returns nil for a target with no endpoint or subscriber" do
      runtime = start_runtime(endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: ->(message) { message })])

      expect(runtime.receiver(topic("other"))).to be_nil
    end

    it "returns nil when the runtime is not running" do
      runtime = ServiceMeshNats::Runtime.new(ServiceMeshNats::Client.new(config, map), config,
        endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: ->(message) { message })])

      expect(runtime.receiver(echo)).to be_nil
    ensure
      runtime.client.close
    end

    it "returns nil after stop" do
      runtime = start_runtime(endpoints: [ServiceMesh::Endpoint.new(target: echo, handler: ->(message) { message })])

      runtime.stop(0)

      expect(runtime.receiver(echo)).to be_nil
    end

    it "returns a subscriber's stream and consumer" do
      client.jetstream.add_stream(name: "OWN", subjects: ["access.own"])
      target = topic("own", stream: "OWN")
      runtime = start_runtime(subscribers: [ServiceMesh::Subscriber.new(target: target, consumer_group: "audit",
        handler: ->(_) {})])

      receiver = runtime.receiver(target)

      expect(receiver.stream_info.config.name).to eq("OWN")
      expect([receiver.consumer_info.stream_name, receiver.consumer_info.name]).to eq(%w[OWN audit])
    end

    it "returns the stream from the subscriber's metadata when it overrides the target's" do
      client.jetstream.add_stream(name: "OVERRIDE", subjects: ["access.override"])
      target = topic("override", stream: "IGNORED")
      runtime = start_runtime(subscribers: [ServiceMesh::Subscriber.new(target: target, consumer_group: "audit",
        metadata: {"stream" => "OVERRIDE"}, handler: ->(_) {})])

      expect(runtime.receiver(target).stream_info.config.name).to eq("OVERRIDE")
    end

    it "names the replacement consumer after an ordered consumer is replaced" do
      client.jetstream.add_stream(name: "REPLACED", subjects: ["access.replaced"])
      target = topic("replaced", stream: "REPLACED")
      runtime = start_runtime(subscribers: [ServiceMesh::Subscriber.new(target: target, consumer_group: "none",
        metadata: {"ack_policy" => "none", "pull_expiry" => "2", "pull_heartbeat" => "0.5"}, handler: ->(_) {})])
      receiver = runtime.receiver(target)
      old_name = receiver.consumer_info.name

      client.jetstream.delete_consumer("REPLACED", old_name)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
      new_name = nil
      until new_name || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        new_name = begin
          receiver.consumer_info.name
        rescue NATS::JetStream::Error::NotFound
          nil
        end
        sleep 0.1
      end

      expect(new_name).not_to be_nil
      expect(new_name).not_to eq(old_name)
    end

    it "raises Closed from a receiver after stop" do
      client.jetstream.add_stream(name: "STOPPED", subjects: ["access.stopped"])
      target = topic("stopped", stream: "STOPPED")
      runtime = start_runtime(subscribers: [ServiceMesh::Subscriber.new(target: target, consumer_group: "audit",
        handler: ->(_) {})])
      receiver = runtime.receiver(target)

      runtime.stop(0)

      expect { receiver.consumer_info }.to raise_error(ServiceMeshNats::Closed)
    end
  end
end
