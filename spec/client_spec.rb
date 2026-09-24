# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Client do
  let(:echo) { ServiceMesh::Target.new(segments: %w[test echo], kind: :route) }
  let(:event) { ServiceMesh::Target.new(segments: %w[test event], kind: :topic) }
  let(:map) { ServiceMesh::ServiceMap.new }

  it "surfaces a connect failure from new" do
    expect { described_class.new({"url" => "nats://127.0.0.1:1", "connect_timeout" => "0.2"}, map) }
      .to raise_error(Errno::ECONNREFUSED)
  end

  describe "against a server", :nats do
    let(:url) { nats_server_url }
    let(:config) { {"url" => url} }
    let(:runtime_config) { config.merge("deployment_group" => "test") }
    let(:quiet) { Logger.new(File::NULL) }
    let(:client) { described_class.new(config, map) }

    after { client.close }

    def serve(endpoints)
      rt = ServiceMeshNats::Runtime.new(described_class.new(config, map), runtime_config, endpoints: endpoints, logger: quiet)
      rt.start
      @runtimes = (@runtimes || []) << rt
      rt
    end

    after { (@runtimes || []).each { |rt| rt.stop(3) } }

    it "keeps the service map" do
      sm = ServiceMesh::ServiceMap.new(targets: [echo])
      standalone = described_class.new(config, sm)
      expect(standalone.service_map).to equal(sm)
    ensure
      standalone&.close
    end

    it "ignores the deployment group" do
      with_group = described_class.new(runtime_config, map)
      expect(with_group.connection).to be_a(NATS::IO::Client)
    ensure
      with_group&.close
    end

    it "rejects a kind mismatch" do
      expect { client.request(ServiceMesh::Message.new(target: event)) }.to raise_error(ServiceMesh::KindMismatch)
    end

    it "rejects a target the transport cannot carry" do
      bad = ServiceMesh::Target.new(segments: ["a b"], kind: :route)
      expect { client.request(ServiceMesh::Message.new(target: bad)) }.to raise_error(ServiceMesh::InvalidTarget)
    end

    it "rejects a bad per-call timeout" do
      expect { client.request(ServiceMesh::Message.new(target: echo), "request_timeout" => "later") }
        .to raise_error(ServiceMeshNats::BadConfig)
    end

    it "raises Closed on request after close" do
      client.close
      expect { client.request(ServiceMesh::Message.new(target: echo)) }.to raise_error(ServiceMeshNats::Closed)
    end

    it "raises Closed on publish after close" do
      client.close
      expect { client.publish(ServiceMesh::Message.new(target: event)) }.to raise_error(ServiceMeshNats::Closed)
    end

    it "raises Closed on connection after close" do
      client.close
      expect { client.connection }.to raise_error(ServiceMeshNats::Closed)
    end

    it "closes twice without effect" do
      client.close
      expect(client.close).to be_nil
    end

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
      slow_client = described_class.new(config.merge("request_timeout" => "0.1"), map)

      expect { slow_client.request(ServiceMesh::Message.new(target: echo)) }.to raise_error(NATS::Timeout)

      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      expect { slow_client.request(ServiceMesh::Message.new(target: echo), "request_timeout" => "0.3") }.to raise_error(NATS::Timeout)
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be >= 0.25
    ensure
      slow_client&.close
    end
  end
end
