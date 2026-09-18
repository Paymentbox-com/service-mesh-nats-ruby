# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Client do
  let(:echo) { ServiceMesh::Target.new(segments: %w[test echo], kind: :route) }

  describe "checks before any connection" do
    let(:client) { described_class.shared(ServiceMeshNats::Settings.parse({}, require_deployment_group: false)) }

    it "rejects a kind mismatch without a connection" do
      topic = ServiceMesh::Target.new(segments: %w[test event], kind: :topic)
      expect { client.request(ServiceMesh::Message.new(target: topic)) }.to raise_error(ServiceMesh::KindMismatch)
    end

    it "rejects a target the transport cannot carry" do
      bad = ServiceMesh::Target.new(segments: ["a b"], kind: :route)
      expect { client.request(ServiceMesh::Message.new(target: bad)) }.to raise_error(ServiceMesh::InvalidTarget)
    end

    it "rejects a bad per-call timeout" do
      expect { client.request(ServiceMesh::Message.new(target: echo), "request_timeout" => "later") }
        .to raise_error(ServiceMeshNats::BadConfig)
    end

    it "raises NotRunning with no connection" do
      expect { client.request(ServiceMesh::Message.new(target: echo)) }.to raise_error(ServiceMeshNats::NotRunning)
    end
  end

  describe "against a server", :nats do
    let(:url) { nats_server_url }
    let(:config) { {"url" => url} }
    let(:runtime_config) { config.merge("deployment_group" => "test") }
    let(:quiet) { Logger.new(File::NULL) }
    let(:client) { described_class.new(config) }
    let(:map) { ServiceMesh::ServiceMap.new }

    after { client.close }

    def serve(endpoints)
      rt = ServiceMeshNats::Runtime.new(runtime_config, map, endpoints: endpoints, logger: quiet)
      rt.start
      @runtimes = (@runtimes || []) << rt
      rt
    end

    after { (@runtimes || []).each { |rt| rt.stop(3) } }

    it "raises the transport's no-responders error when nothing serves the target" do
      expect { client.request(ServiceMesh::Message.new(target: echo)) }.to raise_error(NATS::IO::NoRespondersError)
    end

    it "turns a raising handler into HandlerError" do
      serve([ServiceMesh::Endpoint.new(target: echo, handler: ->(_) { raise "boom" })])
      expect { client.request(ServiceMesh::Message.new(target: echo)) }
        .to raise_error(ServiceMeshNats::HandlerError) { |e| expect(e.text).to eq("boom") }
    end

    it "times out with the transport's timeout, configured and per call" do
      serve([ServiceMesh::Endpoint.new(target: echo, handler: lambda { |m|
        sleep 3
        m
      })])
      slow_client = described_class.new(config.merge("request_timeout" => "0.1"))

      expect { slow_client.request(ServiceMesh::Message.new(target: echo)) }.to raise_error(NATS::Timeout)

      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      expect { slow_client.request(ServiceMesh::Message.new(target: echo), "request_timeout" => "0.3") }.to raise_error(NATS::Timeout)
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be >= 0.25
    ensure
      slow_client&.close
    end
  end
end
