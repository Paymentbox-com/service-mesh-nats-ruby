# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Client do
  let(:echo) { ServiceMeshNats::Target.new(segments: %w[test echo], kind: :route) }
  let(:event) { ServiceMeshNats::Target.new(segments: %w[test event], kind: :topic) }

  describe "checks before any connection" do
    let(:client) { described_class.shared(ServiceMeshNats::Settings.parse({}, require_deployment_group: false)) }

    it "rejects a request to a topic" do
      expect { client.request(ServiceMeshNats::Message.new(target: event)) }.to raise_error(ServiceMeshNats::KindMismatch)
    end

    it "rejects a publish to a route" do
      expect { client.publish(ServiceMeshNats::Message.new(target: echo)) }.to raise_error(ServiceMeshNats::KindMismatch)
    end

    it "rejects a target the transport cannot carry" do
      bad = ServiceMeshNats::Target.new(segments: ["a b"], kind: :route)
      expect { client.request(ServiceMeshNats::Message.new(target: bad)) }.to raise_error(ServiceMeshNats::InvalidTarget)
    end

    it "rejects a bad per-call timeout" do
      expect { client.request(ServiceMeshNats::Message.new(target: echo), "request_timeout" => "later") }
        .to raise_error(ServiceMeshNats::BadConfig)
    end

    it "raises NotRunning with no connection" do
      expect { client.request(ServiceMeshNats::Message.new(target: echo)) }.to raise_error(ServiceMeshNats::NotRunning)
    end
  end

  describe "against a server", :nats do
    let(:url) { nats_server_url }
    let(:config) { {"url" => url} }
    let(:runtime_config) { config.merge("deployment_group" => "test") }
    let(:quiet) { Logger.new(File::NULL) }
    let(:client) { described_class.new(config) }
    let(:map) { ServiceMeshNats::ServiceMap.new }

    after { client.close }

    def serve(endpoints: [], subscribers: [], config: runtime_config)
      rt = ServiceMeshNats::Runtime.new(config, map, endpoints: endpoints, subscribers: subscribers, logger: quiet)
      rt.start
      @runtimes = (@runtimes || []) << rt
      rt
    end

    after { (@runtimes || []).each { |rt| rt.stop(3) } }

    it "round-trips payload and metadata both ways and ignores the reply's target" do
      seen = nil
      serve(endpoints: [ServiceMeshNats::Endpoint.new(target: echo, handler: lambda { |m|
        seen = m
        ServiceMeshNats::Message.new(
          target: ServiceMeshNats::Target.new(segments: %w[nowhere], kind: :topic),
          metadata: {"Reply-Key" => "reply-value"},
          payload: m.payload.upcase
        )
      })])

      reply = client.request(ServiceMeshNats::Message.new(target: echo, metadata: {"Request-Key" => "request-value"}, payload: "hello"))

      expect(seen.target.same_channel?(echo)).to be(true)
      expect(seen.metadata["Request-Key"]).to eq("request-value")
      expect(reply.payload).to eq("HELLO")
      expect(reply.payload.encoding).to eq(Encoding::BINARY)
      expect(reply.metadata["Reply-Key"]).to eq("reply-value")
      expect(reply.target).to eq(echo)
    end

    it "carries an empty payload with no metadata" do
      serve(endpoints: [ServiceMeshNats::Endpoint.new(target: echo, handler: ->(m) { m })])
      reply = client.request(ServiceMeshNats::Message.new(target: echo))
      expect([reply.payload, reply.metadata]).to eq(["", {}])
    end

    it "raises the transport's no-responders error when nothing serves the target" do
      expect { client.request(ServiceMeshNats::Message.new(target: echo)) }.to raise_error(NATS::IO::NoRespondersError)
    end

    it "turns a raising handler into HandlerError" do
      serve(endpoints: [ServiceMeshNats::Endpoint.new(target: echo, handler: ->(_) { raise "boom" })])
      expect { client.request(ServiceMeshNats::Message.new(target: echo)) }
        .to raise_error(ServiceMeshNats::HandlerError) { |e| expect(e.text).to eq("boom") }
    end

    it "times out with the transport's timeout, configured and per call" do
      serve(endpoints: [ServiceMeshNats::Endpoint.new(target: echo, handler: lambda { |m|
        sleep 3
        m
      })])
      slow_client = described_class.new(config.merge("request_timeout" => "0.1"))

      expect { slow_client.request(ServiceMeshNats::Message.new(target: echo)) }.to raise_error(NATS::Timeout)

      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      expect { slow_client.request(ServiceMeshNats::Message.new(target: echo), "request_timeout" => "0.3") }.to raise_error(NATS::Timeout)
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be >= 0.25
    ensure
      slow_client&.close
    end

    it "publishes to a subscriber with metadata" do
      got = Queue.new
      serve(subscribers: [ServiceMeshNats::Subscriber.new(target: event, handler: ->(m) { got << m })])

      client.publish(ServiceMeshNats::Message.new(target: event, metadata: {"Event-Id" => "42"}, payload: "created"))

      m = Timeout.timeout(3) { got.pop }
      expect(m.target.same_channel?(event)).to be(true)
      expect([m.payload, m.metadata["Event-Id"]]).to eq(["created", "42"])
    end

    it "does not close the connection on close when obtained from a runtime" do
      rt = serve(endpoints: [ServiceMeshNats::Endpoint.new(target: echo, handler: ->(m) { m })])
      shared = rt.client
      shared.close
      expect(shared.request(ServiceMeshNats::Message.new(target: echo)).payload).to eq("")
    end
  end
end
