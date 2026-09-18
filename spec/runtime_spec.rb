# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Runtime, :nats do
  let(:url) { nats_server_url }
  let(:echo) { ServiceMeshNats::Target.new(segments: %w[test echo], kind: :route) }
  let(:event) { ServiceMeshNats::Target.new(segments: %w[test event], kind: :topic) }
  let(:map) { ServiceMeshNats::ServiceMap.new }
  let(:quiet) { Logger.new(File::NULL) }

  def config(group = "test")
    {"url" => url, "deployment_group" => group}
  end

  def start(cfg, endpoints: [], subscribers: [])
    rt = described_class.new(cfg, map, endpoints: endpoints, subscribers: subscribers, logger: quiet)
    rt.start
    @runtimes = (@runtimes || []) << rt
    rt
  end

  after { (@runtimes || []).each { |rt| rt.stop(3) } }

  let(:client) { ServiceMeshNats::Client.new("url" => url) }
  after { client.close }

  describe "consumer groups" do
    none = {"consumer_group" => "none"}
    shared = {"consumer_group" => "order-consumers"}

    [
      ["same deployment, key absent: one instance handles it", %w[billing billing], [{}, {}], 1],
      ["different deployments, key absent: each deployment handles it", %w[billing audit], [{}, {}], 2],
      ["same deployment, none: every instance handles it", %w[billing billing], [none, none], 2],
      ["different deployments, shared named group: one instance handles it", %w[billing audit], [shared, shared], 1]
    ].each do |name, groups, metadata, want|
      it name do
        count = Concurrent::AtomicFixnum.new(0)
        2.times do |i|
          sub = ServiceMeshNats::Subscriber.new(target: event, metadata: metadata[i], handler: ->(_) { count.increment })
          start(config(groups[i]), subscribers: [sub])
        end

        client.publish(ServiceMeshNats::Message.new(target: event))

        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 3
        sleep 0.01 while count.value < want && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        sleep 0.1 # let an unwanted extra delivery show up
        expect(count.value).to eq(want)
      end
    end

    it "honours a group set on the target rather than the binding" do
      count = Concurrent::AtomicFixnum.new(0)
      %w[a b].each do |group|
        t = ServiceMeshNats::Target.new(segments: event.segments, kind: :topic, metadata: {"consumer_group" => group})
        start(config("same"), subscribers: [ServiceMeshNats::Subscriber.new(target: t, handler: ->(_) { count.increment })])
      end
      client.publish(ServiceMeshNats::Message.new(target: event))
      Timeout.timeout(3) { sleep 0.01 until count.value == 2 }
    end
  end

  describe "the runtime-owned client" do
    it "works only inside the running window" do
      rt = described_class.new(config, map, endpoints: [ServiceMeshNats::Endpoint.new(target: echo, handler: ->(m) { m })], logger: quiet)
      c = rt.client
      req = ServiceMeshNats::Message.new(target: echo)

      expect { c.request(req) }.to raise_error(ServiceMeshNats::NotRunning)
      rt.start
      expect(c.request(req).payload).to eq("")
      rt.stop(1)
      expect { c.request(req) }.to raise_error(ServiceMeshNats::NotRunning)
    end
  end

  describe "lifecycle" do
    it "moves created -> running -> stopped and does not restart" do
      rt = described_class.new(config, map, logger: quiet)

      expect(rt.running?).to be(false)
      expect(rt.stop(0)).to be(true)
      rt.start
      expect(rt.running?).to be(true)
      expect { rt.start }.to raise_error(ServiceMeshNats::AlreadyStarted)
      expect(rt.stop(1)).to be(true)
      expect(rt.running?).to be(false)
      expect(rt.stop(0)).to be(true)
      expect { rt.start }.to raise_error(ServiceMeshNats::Stopped)
    end

    it "surfaces a connect failure and stays not running" do
      rt = described_class.new({"url" => "nats://127.0.0.1:1", "connect_timeout" => "0.2", "deployment_group" => "t"}, map, logger: quiet)
      expect { rt.start }.to raise_error(Errno::ECONNREFUSED)
      expect(rt.running?).to be(false)
    end

    it "rejects a negative drain" do
      expect { described_class.new(config, map).stop(-1) }.to raise_error(ArgumentError)
    end
  end

  describe "stop" do
    it "waits for an in-flight handler and the requester still gets the reply" do
      entered = Queue.new
      release = Queue.new
      rt = start(config, endpoints: [ServiceMeshNats::Endpoint.new(target: echo, handler: lambda { |_|
        entered << true
        release.pop
        ServiceMeshNats::Message.new(target: echo, payload: "done")
      })])

      reply = Thread.new { client.request(ServiceMeshNats::Message.new(target: echo), "request_timeout" => "5") }
      Timeout.timeout(3) { entered.pop }

      stopped = Thread.new { rt.stop(3) }
      sleep 0.2
      expect(stopped.alive?).to be(true)

      release << true
      expect(stopped.value).to be(true)
      expect(reply.value.payload).to eq("done")
    end

    it "returns false when the drain expires with a handler still running" do
      entered = Queue.new
      release = Queue.new
      rt = start(config, endpoints: [ServiceMeshNats::Endpoint.new(target: echo, handler: lambda { |_|
        entered << true
        release.pop
        ServiceMeshNats::Message.new(target: echo)
      })])

      Thread.new { client.request(ServiceMeshNats::Message.new(target: echo), "request_timeout" => "2") rescue nil } # rubocop:disable Style/RescueModifier
      Timeout.timeout(3) { entered.pop }

      expect(rt.stop(0.1)).to be(false)
      release << true
    end
  end

  describe "concurrency" do
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
      start(config.merge("concurrency" => "2"), endpoints: [ServiceMeshNats::Endpoint.new(target: echo, handler: handler)])

      threads = 6.times.map { Thread.new { client.request(ServiceMeshNats::Message.new(target: echo)) } }
      threads.each(&:value)

      expect(peak.value).to eq(2)
    end
  end
end
