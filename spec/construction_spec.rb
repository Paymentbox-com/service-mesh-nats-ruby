# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Runtime, "construction", :nats do
  let(:config) { {"deployment_group" => "billing"} }
  let(:route) { ServiceMesh::Target.new(segments: %w[a b], kind: :route) }
  let(:topic) { ServiceMesh::Target.new(segments: %w[a b], kind: :topic) }
  let(:ok) { ->(message) { message } }
  let(:map) { ServiceMesh::ServiceMap.new(targets: [route, topic]) }
  let(:client) { ServiceMeshNats::Client.new(nats_config, map) }

  after { client.close }

  def endpoint(target, consumer_group: nil, metadata: {})
    ServiceMesh::Endpoint.new(target: target, consumer_group: consumer_group, metadata: metadata, handler: ok)
  end

  def subscriber(target, consumer_group: nil, metadata: {})
    ServiceMesh::Subscriber.new(target: target, consumer_group: consumer_group, metadata: metadata, handler: ok)
  end

  def build_subscriber(metadata, consumer_group: nil)
    described_class.new(client, config, subscribers: [subscriber(topic, consumer_group: consumer_group, metadata: metadata)])
  end

  it "rejects a target the transport cannot carry" do
    bad = ServiceMesh::Target.new(segments: ["a.b"], kind: :route)
    expect { described_class.new(client, config, endpoints: [endpoint(bad)]) }.to raise_error(ServiceMesh::InvalidTarget)
  end

  it "rejects a handler that cannot be called" do
    ep = ServiceMesh::Endpoint.new(target: route, handler: :not_callable)
    expect { described_class.new(client, config, endpoints: [ep]) }.to raise_error(ArgumentError)
  end

  it "ignores connection keys in its config" do
    runtime = described_class.new(client, config.merge("url" => "nats://127.0.0.1:1", "connect_timeout" => "later"))
    expect(runtime.client).to equal(client)
  end

  it "exposes the client's service map" do
    expect(described_class.new(client, config).service_map).to equal(map)
  end

  it "resolves consumer groups for endpoints and subscribers" do
    runtime = described_class.new(client, config,
      endpoints: [endpoint(route, consumer_group: "none")],
      subscribers: [subscriber(topic.with(segments: %w[c d]))])
    subscriptions = runtime.instance_variable_get(:@subscriptions)
    expect(subscriptions.map(&:queue)).to eq([nil, "billing"])
  end

  describe "JetStream settings" do
    it "rejects stream in an endpoint's metadata" do
      expect { described_class.new(client, config, endpoints: [endpoint(route, metadata: {"stream" => "S"})]) }
        .to raise_error(ServiceMeshNats::BadConfig, /stream/)
    end

    it "rejects stream on an endpoint's target" do
      streamed = route.with(metadata: {"stream" => "S"})
      expect { described_class.new(client, config, endpoints: [endpoint(streamed)]) }
        .to raise_error(ServiceMeshNats::BadConfig, /stream/)
    end

    it "rejects create_consumer false on a subscriber with consumer group none" do
      expect { build_subscriber({"stream" => "S", "create_consumer" => "false"}, consumer_group: "none") }
        .to raise_error(ServiceMeshNats::BadConfig, /create_consumer/)
    end

    it "rejects a create_consumer that is not true or false" do
      expect { build_subscriber({"stream" => "S", "create_consumer" => "yes"}) }
        .to raise_error(ServiceMeshNats::BadConfig, /create_consumer/)
    end

    it "rejects an unknown ack_policy" do
      expect { build_subscriber({"stream" => "S", "ack_policy" => "all"}) }
        .to raise_error(ServiceMeshNats::BadConfig, /ack_policy/)
    end

    it "rejects an ack_wait that is not a positive number of seconds" do
      expect { build_subscriber({"stream" => "S", "ack_wait" => "2m"}) }
        .to raise_error(ServiceMeshNats::BadConfig, /ack_wait/)
    end

    it "rejects a subscriber's max_deliver that is not a positive integer" do
      expect { build_subscriber({"stream" => "S", "max_deliver" => "0"}) }
        .to raise_error(ServiceMeshNats::BadConfig, /max_deliver/)
    end

    it "rejects a runtime max_deliver that is not a positive integer" do
      expect { described_class.new(client, config.merge("max_deliver" => "many")) }
        .to raise_error(ServiceMeshNats::BadConfig, /max_deliver/)
    end

    it "rejects a backoff entry that is not a positive number of seconds" do
      expect { build_subscriber({"stream" => "S", "backoff" => "1,,5"}) }
        .to raise_error(ServiceMeshNats::BadConfig, /backoff/)
    end

    it "rejects a backoff with as many entries as max_deliver" do
      expect { build_subscriber({"stream" => "S", "backoff" => "1,2,3", "max_deliver" => "3"}) }
        .to raise_error(ServiceMeshNats::BadConfig, /max_deliver=3 must exceed the 3 entries of backoff/)
    end

    it "rejects a nak_delay that is not a positive number of seconds" do
      expect { build_subscriber({"stream" => "S", "nak_delay" => "-1"}) }
        .to raise_error(ServiceMeshNats::BadConfig, /nak_delay/)
    end

    it "rejects a max_ack_pending that is not a positive integer" do
      expect { build_subscriber({"stream" => "S", "max_ack_pending" => "1.5"}) }
        .to raise_error(ServiceMeshNats::BadConfig, /max_ack_pending/)
    end

    it "rejects an unknown deliver_policy" do
      expect { build_subscriber({"stream" => "S", "deliver_policy" => "last"}) }
        .to raise_error(ServiceMeshNats::BadConfig, /deliver_policy/)
    end

    it "rejects a consumer_inactive_threshold that is not a positive number of seconds" do
      expect { build_subscriber({"stream" => "S", "consumer_inactive_threshold" => "7d"}) }
        .to raise_error(ServiceMeshNats::BadConfig, /consumer_inactive_threshold/)
    end

    it "rejects a subscriber's pull_expiry that is not a positive number of seconds" do
      expect { build_subscriber({"stream" => "S", "pull_expiry" => "30s"}) }
        .to raise_error(ServiceMeshNats::BadConfig, /pull_expiry/)
    end

    it "rejects a subscriber's pull_heartbeat that is not a positive number of seconds" do
      expect { build_subscriber({"stream" => "S", "pull_heartbeat" => "0"}) }
        .to raise_error(ServiceMeshNats::BadConfig, /pull_heartbeat/)
    end

    it "rejects a runtime pull_expiry that is not a positive number of seconds" do
      expect { described_class.new(client, config.merge("pull_expiry" => "soon")) }
        .to raise_error(ServiceMeshNats::BadConfig, /pull_expiry/)
    end

    it "rejects a runtime pull_heartbeat that is not a positive number of seconds" do
      expect { described_class.new(client, config.merge("pull_heartbeat" => "-5")) }
        .to raise_error(ServiceMeshNats::BadConfig, /pull_heartbeat/)
    end

    it "accepts a subscriber with every option set" do
      metadata = {
        "stream" => "S",
        "create_consumer" => "false",
        "ack_policy" => "explicit",
        "ack_wait" => "2.5",
        "max_deliver" => "4",
        "backoff" => "1, 2, 3",
        "nak_delay" => "0.5",
        "max_ack_pending" => "10",
        "deliver_policy" => "new",
        "consumer_inactive_threshold" => "60",
        "pull_expiry" => "20",
        "pull_heartbeat" => "2.5"
      }
      expect(build_subscriber(metadata, consumer_group: "audit")).to be_a(described_class)
    end

    it "takes a subscriber's metadata over its target's" do
      bad = topic.with(metadata: {"stream" => "S", "ack_policy" => "bad"})
      subscriber = subscriber(bad, metadata: {"ack_policy" => "none"})
      expect(described_class.new(client, config, subscribers: [subscriber])).to be_a(described_class)
    end

    it "ignores JetStream settings on a subscriber without stream" do
      expect(build_subscriber({"ack_policy" => "bad"})).to be_a(described_class)
    end
  end
end
