# frozen_string_literal: true

RSpec.shared_context "service_mesh_nats conformance setup" do
  let(:quiet) { Logger.new(File::NULL) }
  let(:new_runtime) do
    lambda do |client, config, endpoints:, subscribers:|
      ServiceMeshNats::Runtime.new(client, config, endpoints: endpoints, subscribers: subscribers, logger: quiet)
    end
  end
  let(:new_client) { ->(config, map) { ServiceMeshNats::Client.new(config, map) } }
  let(:runtime_config) { nats_config("deployment_group" => "test") }
  let(:client_config) { nats_config }
  let(:route_target) { ServiceMesh::Target.new(segments: %w[test echo], kind: :route) }
  let(:topic_target) { ServiceMesh::Target.new(segments: %w[test event], kind: :topic, metadata: target_metadata) }
end

RSpec.describe "service_mesh_nats conformance", :nats do
  it_behaves_like "a service mesh transport" do
    include_context "service_mesh_nats conformance setup"
  end

  describe "in JetStream mode" do
    # A fresh stream for each example, so a consumer never sees an earlier
    # example's messages.
    before do
      admin = NATS.connect(nats_server_url)
      jetstream = admin.jetstream
      begin
        jetstream.delete_stream("CONFORMANCE")
      rescue NATS::JetStream::Error::NotFound
        nil
      end
      jetstream.add_stream(name: "CONFORMANCE", subjects: ["test.event"])
    ensure
      admin&.close
    end

    it_behaves_like "a service mesh transport" do
      include_context "service_mesh_nats conformance setup"
      # The suite's own topic targets take target_metadata, and topic_target
      # above takes it too. Route targets take none, because a stream on a
      # route target is rejected.
      let(:target_metadata) { {"stream" => "CONFORMANCE"} }
    end
  end
end
