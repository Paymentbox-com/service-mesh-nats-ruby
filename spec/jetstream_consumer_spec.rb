# frozen_string_literal: true

require "json"

RSpec.describe ServiceMeshNats::Runtime, "JetStream consumers", :nats do
  let(:map) { ServiceMesh::ServiceMap.new }
  let(:config) { nats_config("deployment_group" => "test") }
  let(:quiet) { Logger.new(File::NULL) }
  let(:admin) { ServiceMeshNats::Client.new(config, map) }
  let(:js) { admin.jetstream }

  after do
    (@runtimes || []).each { |runtime| runtime.stop(1) }
    admin.close
  end

  def topic(*segments)
    ServiceMesh::Target.new(segments: segments, kind: :topic)
  end

  def build(target, metadata, consumer_group: nil, runtime_config: config)
    subscriber = ServiceMesh::Subscriber.new(target: target, consumer_group: consumer_group, metadata: metadata, handler: ->(_) {})
    runtime = described_class.new(ServiceMeshNats::Client.new(config, map), runtime_config, subscribers: [subscriber], logger: quiet)
    (@runtimes ||= []) << runtime
    runtime
  end

  def start(...)
    build(...).tap(&:start)
  end

  def consumer_names(stream)
    reply = admin.connection.request("$JS.API.CONSUMER.NAMES.#{stream}", "", timeout: 2)
    JSON.parse(reply.data)["consumers"]
  end

  it "fails start when the stream does not exist" do
    runtime = build(topic("cons", "nostream"), {"stream" => "NOSTREAM"}, consumer_group: "audit")
    expect { runtime.start }.to raise_error(NATS::JetStream::Error::StreamNotFound)
  end

  it "creates a named group's consumer under the group's name with the subscriber's settings" do
    js.add_stream(name: "NAMED", subjects: ["cons.named"])

    start(topic("cons", "named"), {"stream" => "NAMED", "ack_wait" => "7", "max_deliver" => "3"}, consumer_group: "audit")

    consumer = js.consumer_info("NAMED", "audit").config
    expect([consumer.filter_subject, consumer.ack_wait, consumer.max_deliver]).to eq(["cons.named", 7, 3])
  end

  it "updates a named group's consumer when a later start changes its settings" do
    js.add_stream(name: "UPDATED", subjects: ["cons.updated"])
    start(topic("cons", "updated"), {"stream" => "UPDATED", "ack_wait" => "7"}, consumer_group: "audit").stop(1)

    start(topic("cons", "updated"), {"stream" => "UPDATED", "ack_wait" => "9"}, consumer_group: "audit")

    expect(js.consumer_info("UPDATED", "audit").config.ack_wait).to eq(9)
  end

  it "fails start when a later start changes a named group's deliver_policy" do
    js.add_stream(name: "CHANGEDPOLICY", subjects: ["cons.changedpolicy"])
    start(topic("cons", "changedpolicy"), {"stream" => "CHANGEDPOLICY", "deliver_policy" => "new"}, consumer_group: "audit").stop(1)

    runtime = build(topic("cons", "changedpolicy"), {"stream" => "CHANGEDPOLICY", "deliver_policy" => "all"}, consumer_group: "audit")

    expect { runtime.start }.to raise_error(NATS::JetStream::Error::ServerError, /deliver policy can not be updated/)
  end

  it "binds to an existing consumer without changing it when create_consumer is false" do
    js.add_stream(name: "EXTERNAL", subjects: ["cons.external"])
    js.add_consumer("EXTERNAL", durable_name: "audit", filter_subject: "cons.external", ack_wait: 11)

    start(topic("cons", "external"), {"stream" => "EXTERNAL", "create_consumer" => "false", "ack_wait" => "3"}, consumer_group: "audit")

    expect(js.consumer_info("EXTERNAL", "audit").config.ack_wait).to eq(11)
  end

  it "fails start when create_consumer is false and the consumer does not exist" do
    js.add_stream(name: "UNMANAGED", subjects: ["cons.unmanaged"])
    runtime = build(topic("cons", "unmanaged"), {"stream" => "UNMANAGED", "create_consumer" => "false"}, consumer_group: "audit")

    expect { runtime.start }.to raise_error(NATS::JetStream::Error::ConsumerNotFound)
  end

  it "gives each instance with consumer group none its own consumer" do
    js.add_stream(name: "EACH", subjects: ["cons.each"])

    2.times { start(topic("cons", "each"), {"stream" => "EACH"}, consumer_group: "none") }

    expect(consumer_names("EACH").length).to eq(2)
  end

  it "defaults a consumer's max_deliver to 5" do
    js.add_stream(name: "DEFAULTED", subjects: ["cons.defaulted"])

    start(topic("cons", "defaulted"), {"stream" => "DEFAULTED"}, consumer_group: "audit")

    expect(js.consumer_info("DEFAULTED", "audit").config.max_deliver).to eq(5)
  end

  it "takes a consumer's default max_deliver from the runtime config" do
    js.add_stream(name: "CONFIGURED", subjects: ["cons.configured"])

    start(topic("cons", "configured"), {"stream" => "CONFIGURED"}, consumer_group: "audit",
      runtime_config: config.merge("max_deliver" => "8"))

    expect(js.consumer_info("CONFIGURED", "audit").config.max_deliver).to eq(8)
  end

  it "fails start when pull_expiry is under 1 second" do
    runtime = build(topic("cons", "limits"), {"stream" => "LIMITS", "pull_expiry" => "0.9"}, consumer_group: "audit")
    expect { runtime.start }.to raise_error(ServiceMeshNats::BadConfig, /pull_expiry=0.9 must be at least 1 second/)
  end

  it "fails start when pull_heartbeat is under 0.5 seconds" do
    runtime = build(topic("cons", "limits"), {"stream" => "LIMITS", "pull_heartbeat" => "0.4"}, consumer_group: "audit")
    expect { runtime.start }.to raise_error(ServiceMeshNats::BadConfig, /pull_heartbeat=0.4 must be from 0.5 to 30 seconds/)
  end

  it "fails start when pull_heartbeat is over 30 seconds" do
    runtime = build(topic("cons", "limits"), {"stream" => "LIMITS", "pull_expiry" => "120", "pull_heartbeat" => "31"}, consumer_group: "audit")
    expect { runtime.start }.to raise_error(ServiceMeshNats::BadConfig, /pull_heartbeat=31.0 must be from 0.5 to 30 seconds/)
  end

  it "fails start when pull_heartbeat is over half of pull_expiry" do
    runtime = build(topic("cons", "limits"), {"stream" => "LIMITS", "pull_expiry" => "10", "pull_heartbeat" => "5.5"}, consumer_group: "audit")
    expect { runtime.start }.to raise_error(ServiceMeshNats::BadConfig, /pull_heartbeat=5.5 must be no more than half of pull_expiry=10.0/)
  end

  it "fails start when the runtime config's pull_expiry is under 1 second" do
    runtime = build(topic("cons", "limits"), {"stream" => "LIMITS"}, consumer_group: "audit",
      runtime_config: config.merge("pull_expiry" => "0.5"))
    expect { runtime.start }.to raise_error(ServiceMeshNats::BadConfig, /pull_expiry=0.5 must be at least 1 second/)
  end

  it "gives an ordered consumer a 5 minute inactive threshold" do
    js.add_stream(name: "ORDERED", subjects: ["cons.ordered"])

    start(topic("cons", "ordered"), {"stream" => "ORDERED", "ack_policy" => "none"}, consumer_group: "none")

    name = consumer_names("ORDERED").first
    expect(js.consumer_info("ORDERED", name).config.inactive_threshold).to eq(300)
  end
end
