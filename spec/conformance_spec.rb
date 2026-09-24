# frozen_string_literal: true

RSpec.describe "service_mesh_nats conformance", :nats do
  it_behaves_like "a service mesh transport" do
    let(:quiet) { Logger.new(File::NULL) }
    let(:new_runtime) do
      lambda do |client, config, endpoints:, subscribers:|
        ServiceMeshNats::Runtime.new(client, config, endpoints: endpoints, subscribers: subscribers, logger: quiet)
      end
    end
    let(:new_client) { ->(config, map) { ServiceMeshNats::Client.new(config, map) } }
    let(:runtime_config) { {"url" => nats_server_url, "deployment_group" => "test"} }
    let(:client_config) { {"url" => nats_server_url} }
    let(:route_target) { ServiceMesh::Target.new(segments: %w[test echo], kind: :route) }
    let(:topic_target) { ServiceMesh::Target.new(segments: %w[test event], kind: :topic) }
  end
end
