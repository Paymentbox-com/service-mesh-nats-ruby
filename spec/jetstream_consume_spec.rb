# frozen_string_literal: true

require "json"
require "stringio"
require "time"

RSpec.describe ServiceMeshNats::Runtime, "JetStream consuming", :nats do
  let(:map) { ServiceMesh::ServiceMap.new }
  let(:config) { nats_config("deployment_group" => "test") }
  let(:admin) { ServiceMeshNats::Client.new(config, map) }
  let(:jetstream) { admin.jetstream }
  let(:log) { StringIO.new }

  after do
    (@runtimes || []).each { |runtime| runtime.stop(1) }
    admin.close
  end

  # A topic target whose stream, named +name+, holds only its subject.
  def stream_target(name)
    subject = "consume.#{name.downcase}"
    jetstream.add_stream(name: name, subjects: [subject])
    ServiceMesh::Target.new(segments: subject.split("."), kind: :topic, metadata: {"stream" => name})
  end

  def subscriber(target, handler, consumer_group: "audit", metadata: {})
    ServiceMesh::Subscriber.new(target: target, consumer_group: consumer_group, metadata: metadata, handler: handler)
  end

  def start_runtime(subscribers: [], endpoints: [], runtime_config: config)
    runtime = described_class.new(ServiceMeshNats::Client.new(runtime_config, map), runtime_config,
      subscribers: subscribers, endpoints: endpoints, logger: Logger.new(log))
    runtime.start
    (@runtimes ||= []) << runtime
    runtime
  end

  def publish(target, payload)
    admin.publish(ServiceMesh::Message.new(target: target, payload: payload))
  end

  # Every message published to +subject+ from now on, as it arrives.
  def spy(subject)
    seen = Queue.new
    admin.connection.subscribe(subject) { |msg| seen << msg }
    admin.connection.flush
    seen
  end

  def pull_requests(stream)
    requests = Queue.new
    admin.connection.subscribe("$JS.API.CONSUMER.MSG.NEXT.#{stream}.>") { |msg| requests << JSON.parse(msg.data) }
    admin.connection.flush
    requests
  end

  def consumer_names(stream)
    reply = admin.connection.request("$JS.API.CONSUMER.NAMES.#{stream}", "", timeout: 2)
    JSON.parse(reply.data)["consumers"]
  end

  def drain(queue, seconds)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
    items = []
    loop do
      left = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
      break if left <= 0

      item = queue.pop(timeout: left)
      break if item.nil?

      items << item
    end
    items
  end

  def wait_for(seconds = 5)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
    sleep 0.05 until yield || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
    yield
  end

  describe "consumer groups" do
    it "splits a named group's messages between its two instances" do
      target = stream_target("SPLIT")
      seen = Queue.new
      2.times do |instance|
        handler = lambda do |message|
          seen << [instance, message.payload]
          sleep 0.05
        end
        start_runtime(subscribers: [subscriber(target, handler, consumer_group: "workers")],
          runtime_config: config.merge("concurrency" => "1"))
      end

      10.times { |index| publish(target, index.to_s) }
      deliveries = drain(seen, 3)

      expect(deliveries.map(&:last).sort_by(&:to_i)).to eq((0..9).map(&:to_s))
      expect(deliveries.map(&:first).uniq.sort).to eq([0, 1])
    end

    it "gives each of two groups every message" do
      target = stream_target("GROUPS")
      seen = {"billing" => Queue.new, "audit" => Queue.new}
      seen.each do |group, queue|
        start_runtime(subscribers: [subscriber(target, ->(message) { queue << message.payload }, consumer_group: group)])
      end

      3.times { |index| publish(target, index.to_s) }

      expect(seen.transform_values { |queue| drain(queue, 1).sort }).to eq("billing" => %w[0 1 2], "audit" => %w[0 1 2])
    end

    it "gives every instance with consumer group none every message" do
      target = stream_target("EVERYONE")
      seen = [Queue.new, Queue.new]
      seen.each do |queue|
        start_runtime(subscribers: [subscriber(target, ->(message) { queue << message.payload }, consumer_group: "none")])
      end

      3.times { |index| publish(target, index.to_s) }

      expect(seen.map { |queue| drain(queue, 1).sort }).to eq([%w[0 1 2], %w[0 1 2]])
    end
  end

  describe "ack policies" do
    it "acks with double by sending each ack as a request with a reply subject" do
      target = stream_target("DOUBLE")
      acks = spy("$JS.ACK.DOUBLE.>")
      start_runtime(subscribers: [subscriber(target, ->(_) {})])

      publish(target, "x")

      expect(acks.pop(timeout: 3).reply).not_to be_nil
      expect(wait_for { jetstream.consumer_info("DOUBLE", "audit").num_ack_pending.zero? }).to be(true)
    end

    it "acks with explicit by publishing each ack without a reply subject" do
      target = stream_target("EXPLICIT")
      acks = spy("$JS.ACK.EXPLICIT.>")
      start_runtime(subscribers: [subscriber(target, ->(_) {}, metadata: {"ack_policy" => "explicit"})])

      publish(target, "x")

      ack = acks.pop(timeout: 3)
      expect([ack.data, ack.reply]).to eq(["+ACK", nil])
    end

    it "settles nothing and sends nothing again with ack_policy none" do
      target = stream_target("NOACKS")
      attempts = Queue.new
      handler = lambda do |message|
        attempts << message.metadata[ServiceMesh::DELIVERY_ATTEMPT_KEY]
        raise "boom"
      end
      start_runtime(subscribers: [subscriber(target, handler, metadata: {"ack_policy" => "none", "ack_wait" => "1"})])

      publish(target, "x")

      expect(drain(attempts, 2)).to eq(["1"])
      expect(jetstream.consumer_info("NOACKS", "audit").config.ack_policy).to eq("none")
    end
  end

  describe "failures" do
    it "delivers a failing handler's message again until max_deliver" do
      target = stream_target("RETRIES")
      attempts = Queue.new
      handler = lambda do |message|
        attempts << message.metadata[ServiceMesh::DELIVERY_ATTEMPT_KEY]
        raise "boom"
      end
      start_runtime(subscribers: [subscriber(target, handler, metadata: {"max_deliver" => "3"})])

      publish(target, "x")

      expect(drain(attempts, 2)).to eq(%w[1 2 3])
    end

    it "waits nak_delay before a failed message is delivered again" do
      target = stream_target("NAKDELAY")
      arrivals = Queue.new
      handler = lambda do |_|
        arrivals << Process.clock_gettime(Process::CLOCK_MONOTONIC)
        raise "boom"
      end
      start_runtime(subscribers: [subscriber(target, handler, metadata: {"nak_delay" => "1", "max_deliver" => "2"})])

      publish(target, "x")
      first, second = drain(arrivals, 3)

      expect(second - first).to be >= 0.9
    end

    it "logs an error when a handler fails on the last delivery" do
      target = stream_target("LASTTRY")
      start_runtime(subscribers: [subscriber(target, ->(_) { raise "boom" }, metadata: {"max_deliver" => "2"})])

      publish(target, "x")

      expect(wait_for { log.string.include?("will not be delivered again") }).to be(true)
      expect(log.string).to include("service_mesh_nats: message will not be delivered again subject=consume.lasttry " \
        "stream=LASTTRY consumer=audit stream_sequence=1 delivery_attempt=2 max_deliver=2")
    end

    it "does not log a final failure for a delivery before the last" do
      target = stream_target("NOTLAST")
      attempts = Queue.new
      handler = lambda do |message|
        attempts << message.metadata[ServiceMesh::DELIVERY_ATTEMPT_KEY]
        raise "boom" if message.metadata[ServiceMesh::DELIVERY_ATTEMPT_KEY] == "1"
      end
      start_runtime(subscribers: [subscriber(target, handler, metadata: {"max_deliver" => "2"})])

      publish(target, "x")

      expect(drain(attempts, 1.5)).to eq(%w[1 2])
      expect(log.string).not_to include("will not be delivered again")
    end
  end

  describe "delivery metadata" do
    it "gives the handler each delivery metadata key" do
      target = stream_target("METADATA")
      seen = Queue.new
      start_runtime(subscribers: [subscriber(target, ->(message) { seen << message.metadata }, metadata: {"ack_wait" => "2"})])

      before = Time.now
      publish(target, "x")
      metadata = seen.pop(timeout: 3)

      expect(metadata.slice(ServiceMesh::DELIVERY_ATTEMPT_KEY, ServiceMeshNats::STREAM_METADATA_KEY,
        ServiceMeshNats::CONSUMER_METADATA_KEY, ServiceMeshNats::STREAM_SEQUENCE_METADATA_KEY,
        ServiceMeshNats::CONSUMER_SEQUENCE_METADATA_KEY, ServiceMeshNats::NUM_PENDING_METADATA_KEY)).to eq(
          "Mesh-Delivery-Attempt" => "1", "Mesh-Nats-Stream" => "METADATA", "Mesh-Nats-Consumer" => "audit",
          "Mesh-Nats-Stream-Sequence" => "1", "Mesh-Nats-Consumer-Sequence" => "1", "Mesh-Nats-Num-Pending" => "0"
        )
      expect(Time.iso8601(metadata[ServiceMeshNats::TIMESTAMP_METADATA_KEY])).to be_within(1).of(before)
      expect(Time.iso8601(metadata[ServiceMesh::DEADLINE_KEY])).to be_within(0.3).of(before + 2)
    end

    it "takes a delivery's deadline from its own backoff entry" do
      target = stream_target("BACKOFF")
      deadlines = Queue.new
      handler = lambda do |message|
        deadlines << [Time.now, Time.iso8601(message.metadata[ServiceMesh::DEADLINE_KEY])]
        raise "boom" if message.metadata[ServiceMesh::DELIVERY_ATTEMPT_KEY] == "1"
      end
      start_runtime(subscribers: [subscriber(target, handler, metadata: {"backoff" => "1,3", "max_deliver" => "3"})])

      publish(target, "x")
      first, second = drain(deadlines, 2)

      expect(first[1] - first[0]).to be_within(0.3).of(1)
      expect(second[1] - second[0]).to be_within(0.3).of(3)
    end

    it "gives no deadline for a consumer created outside the runtime that does not ack" do
      target = stream_target("UNACKED")
      jetstream.add_consumer("UNACKED", durable_name: "audit", filter_subject: "consume.unacked", ack_policy: "none")
      seen = Queue.new
      start_runtime(subscribers: [subscriber(target, ->(message) { seen << message.metadata },
        metadata: {"create_consumer" => "false"})])

      publish(target, "x")
      metadata = seen.pop(timeout: 3)

      expect(metadata).to include(ServiceMesh::DELIVERY_ATTEMPT_KEY => "1")
      expect(metadata).not_to have_key(ServiceMesh::DEADLINE_KEY)
    end

    it "logs a warning when a handler starts after its ack deadline" do
      target = stream_target("LATE")
      handler = ->(message) { sleep 1.5 if message.payload == "first" }
      start_runtime(subscribers: [subscriber(target, handler, metadata: {"ack_wait" => "1"})],
        runtime_config: config.merge("concurrency" => "1"))

      publish(target, "first")
      publish(target, "second")

      expect(wait_for { log.string.include?("after its ack deadline") }).to be(true)
      expect(log.string).to match(/service_mesh_nats: handler started after its ack deadline subject=consume\.late stream=LATE consumer=audit stream_sequence=2 delivery_attempt=1 late_by=\d+\.\d+ skipped=0/)
    end

    it "logs a late start at most once a minute for each subscriber, counting the ones it skipped" do
      stub_const("ServiceMeshNats::PullLoop::LATE_START_LOG_INTERVAL", 0.2)
      pull_loop = ServiceMeshNats.const_get(:PullLoop).new(connection: nil, subject: "consume.late", settings: nil,
        consumer: nil, concurrency: 1, permits: nil, logger: nil)

      counts = [pull_loop.send(:late_start_logged), pull_loop.send(:late_start_logged), pull_loop.send(:late_start_logged)]
      sleep 0.25
      counts << pull_loop.send(:late_start_logged)

      expect(counts).to eq([0, nil, nil, 2])
    end
  end

  describe "pulling" do
    it "asks for concurrency messages first, then the difference once fewer than half are owed or waiting" do
      target = stream_target("BATCHES")
      requests = pull_requests("BATCHES")
      start_runtime(subscribers: [subscriber(target, ->(_) {})], runtime_config: config.merge("concurrency" => "4"))

      first = requests.pop(timeout: 3)
      6.times { |index| publish(target, index.to_s) }
      second = requests.pop(timeout: 3)

      expect([first["batch"], second["batch"]]).to eq([4, 3])
    end

    it "sends pull_expiry and pull_heartbeat with each pull" do
      target = stream_target("PULLKEYS")
      requests = pull_requests("PULLKEYS")
      start_runtime(subscribers: [subscriber(target, ->(_) {}, metadata: {"pull_expiry" => "2", "pull_heartbeat" => "0.5"})])

      request = requests.pop(timeout: 3)

      expect(request.slice("expires", "idle_heartbeat")).to eq("expires" => 2_000_000_000, "idle_heartbeat" => 500_000_000)
    end

    it "defaults the heartbeat to half of the expiry" do
      target = stream_target("HALF")
      requests = pull_requests("HALF")
      start_runtime(subscribers: [subscriber(target, ->(_) {})])

      expect(requests.pop(timeout: 3)["idle_heartbeat"]).to eq(15_000_000_000)
    end

    it "caps the default heartbeat at 30 seconds" do
      target = stream_target("CAPPED")
      requests = pull_requests("CAPPED")
      start_runtime(subscribers: [subscriber(target, ->(_) {}, metadata: {"pull_expiry" => "120"})])

      expect(requests.pop(timeout: 3)["idle_heartbeat"]).to eq(30_000_000_000)
    end

    it "defaults an ordered consumer's heartbeat to 5 seconds" do
      target = stream_target("ORDEREDHB")
      requests = pull_requests("ORDEREDHB")
      start_runtime(subscribers: [subscriber(target, ->(_) {}, consumer_group: "none", metadata: {"ack_policy" => "none"})])

      expect(requests.pop(timeout: 3)["idle_heartbeat"]).to eq(5_000_000_000)
    end

    it "defaults an ordered consumer's heartbeat to half of an expiry under 10 seconds" do
      target = stream_target("ORDEREDSHORT")
      requests = pull_requests("ORDEREDSHORT")
      start_runtime(subscribers: [subscriber(target, ->(_) {}, consumer_group: "none",
        metadata: {"ack_policy" => "none", "pull_expiry" => "4"})])

      expect(requests.pop(timeout: 3)["idle_heartbeat"]).to eq(2_000_000_000)
    end

    it "makes a core request wait for a permit held by a JetStream handler" do
      target = stream_target("PERMITS")
      route = ServiceMesh::Target.new(segments: %w[consume echo], kind: :route)
      gate = Queue.new
      started = Queue.new
      handler = lambda do |_|
        started << true
        gate.pop
      end
      start_runtime(subscribers: [subscriber(target, handler)],
        endpoints: [ServiceMesh::Endpoint.new(target: route, handler: ->(message) { message })],
        runtime_config: config.merge("concurrency" => "1"))
      publish(target, "x")
      started.pop(timeout: 3)

      replies = Queue.new
      Thread.new { replies << admin.request(ServiceMesh::Message.new(target: route, payload: "ping")).payload }

      expect(replies.pop(timeout: 0.5)).to be_nil
      gate << true
      expect(replies.pop(timeout: 3)).to eq("ping")
    end
  end

  describe "missing consumers" do
    let(:fast_heartbeat) { {"pull_expiry" => "2", "pull_heartbeat" => "0.5"} }
    let(:seen) { Queue.new }
    let(:missing_line) { "service_mesh_nats: consumer missing subject=consume.missing stream=MISSING consumer=audit" }

    # Starts a runtime whose named group "audit" consumes from the stream
    # MISSING, connected through +proxy+ when one is given, and waits for its
    # first pull to be open. Nothing may subscribe to the pull subject, or the
    # server sends no 503 for a pull to a missing consumer.
    def start_audit(proxy = nil)
      target = stream_target("MISSING")
      runtime_config = proxy ? config.merge("url" => proxy.url) : config
      start_runtime(subscribers: [subscriber(target, ->(message) { seen << message.payload }, metadata: fast_heartbeat)],
        runtime_config: runtime_config)
      sleep 0.5
      target
    end

    def missing_logs
      log.string.scan(missing_line).length
    end

    it "pulls again after missed heartbeats" do
      target = stream_target("HEARTBEATS")
      start_runtime(subscribers: [subscriber(target, ->(message) { seen << message.payload },
        metadata: {"pull_expiry" => "5", "pull_heartbeat" => "0.5"})])

      freeze_nats_server(1.5)
      publish(target, "after")

      expect(seen.pop(timeout: 5)).to eq("after")
      expect(log.string).to include("service_mesh_nats: consume error subject=consume.heartbeats stream=HEARTBEATS " \
        "error=no message or heartbeat for 1.0 seconds")
    end

    it "logs a consumer deleted while connected, and pulls nothing after" do
      start_audit
      jetstream.delete_consumer("MISSING", "audit")
      expect(wait_for { missing_logs.positive? }).to be(true)

      requests = pull_requests("MISSING")

      expect(drain(requests, 1.5)).to eq([])
    end

    it "logs a consumer deleted while disconnected once it reconnects" do
      proxy = nats_proxy
      start_audit(proxy)

      proxy.cut { jetstream.delete_consumer("MISSING", "audit") }

      expect(wait_for(10) { missing_logs.positive? }).to be(true)
    end

    it "logs a missing consumer again while it is still missing" do
      stub_const("ServiceMeshNats::PullLoop::MISSING_LOG_INTERVAL", 0.3)
      start_audit

      jetstream.delete_consumer("MISSING", "audit")

      expect(wait_for { missing_logs >= 3 }).to be(true)
    end

    it "still logs a consumer it stopped pulling after it is created again" do
      stub_const("ServiceMeshNats::PullLoop::MISSING_LOG_INTERVAL", 0.3)
      start_audit
      jetstream.delete_consumer("MISSING", "audit")
      expect(wait_for { missing_logs.positive? }).to be(true)

      jetstream.add_consumer("MISSING", durable_name: "audit", filter_subject: "consume.missing")
      logged = missing_logs

      expect(wait_for { missing_logs >= logged + 2 }).to be(true)
    end

    it "stops logging a consumer it still pulls once it is created again, and resumes consuming" do
      stub_const("ServiceMeshNats::PullLoop::MISSING_LOG_INTERVAL", 0.3)
      proxy = nats_proxy
      target = start_audit(proxy)
      proxy.cut { jetstream.delete_consumer("MISSING", "audit") }
      expect(wait_for(10) { missing_logs.positive? }).to be(true)

      jetstream.add_consumer("MISSING", durable_name: "audit", filter_subject: "consume.missing")
      sleep 1
      logged = missing_logs
      sleep 1
      publish(target, "back")

      expect(missing_logs).to eq(logged)
      expect(seen.pop(timeout: 5)).to eq("back")
    end
  end

  describe "ordered consumers" do
    # The short heartbeat makes a pull dropped during a delete show quickly.
    let(:ordered) { {"ack_policy" => "none", "pull_expiry" => "5", "pull_heartbeat" => "0.5"} }

    def ordered_subscriber(target, seen)
      subscriber(target, ->(message) { seen << message.payload }, consumer_group: "none", metadata: ordered)
    end

    # The consumer's config as the server reports it.
    def consumer_config(stream, name)
      reply = admin.connection.request("$JS.API.CONSUMER.INFO.#{stream}.#{name}", "", timeout: 2)
      JSON.parse(reply.data).fetch("config")
    end

    it "keeps an ordered consumer in memory with one replica" do
      target = stream_target("MEMORY")
      start_runtime(subscribers: [ordered_subscriber(target, Queue.new)])

      config = consumer_config("MEMORY", consumer_names("MEMORY").first)
      expect([config["mem_storage"], config["num_replicas"]]).to eq([true, 1])
    end

    it "loses no message and repeats none when its consumer is deleted mid-stream" do
      target = stream_target("ORDEREDLOST")
      seen = Queue.new
      start_runtime(subscribers: [ordered_subscriber(target, seen)])
      5.times { |index| publish(target, index.to_s) }
      before = drain(seen, 1)

      jetstream.delete_consumer("ORDEREDLOST", consumer_names("ORDEREDLOST").first)
      5.times { |index| publish(target, (index + 5).to_s) }

      expect((before + drain(seen, 3)).sort_by(&:to_i)).to eq((0..9).map(&:to_s))
      expect(log.string).to include("service_mesh_nats: replacing the ordered consumer subject=consume.orderedlost")
      expect(log.string).not_to include("consumer missing")
    end

    it "replaces its consumer at the next stream sequence after a gap" do
      target = stream_target("GAP")
      seen = Queue.new
      runtime = start_runtime(subscribers: [ordered_subscriber(target, seen)])
      3.times { |index| publish(target, index.to_s) }
      drain(seen, 1)
      old_name = consumer_names("GAP").first

      # The server rejects a client's message with a $JS.ACK reply subject, so
      # the delivery with consumer sequence 9, where 4 is next, goes straight
      # onto the pull loop's queue.
      pull_loop = runtime.instance_variable_get(:@pull_loops).values.first
      reply = "$JS.ACK.GAP.#{old_name}.1.20.9.#{Time.now.to_i * 1_000_000_000}.0"
      fake = NATS::Msg.new(subject: "consume.gap", reply: reply, data: "fake")
      pull_loop.instance_variable_get(:@queue) << [pull_loop.instance_variable_get(:@inbox), fake, Time.now]
      expect(wait_for { consumer_names("GAP") != [old_name] }).to be(true)
      publish(target, "after")

      expect(drain(seen, 2)).to eq(["after"])
      expect(log.string).to include("service_mesh_nats: replacing the ordered consumer subject=consume.gap stream=GAP " \
        "consumer=#{old_name} reason=expected consumer sequence 4, got 9")
      names = consumer_names("GAP")
      expect(names).not_to include(old_name)
      config = consumer_config("GAP", names.first)
      expect([config["deliver_policy"], config["opt_start_seq"]]).to eq(["by_start_sequence", 4])
    end
  end
end
