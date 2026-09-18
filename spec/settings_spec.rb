# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Settings do
  let(:base) { {"deployment_group" => "d"} }

  it "applies defaults" do
    s = described_class.parse(base, require_deployment_group: true)
    expect(s.url).to eq("nats://127.0.0.1:4222")
    expect(s.connect_timeout).to eq(5.0)
    expect(s.request_timeout).to eq(30.0)
    expect(s.concurrency).to be > 0
    expect(s.deployment_group).to eq("d")
  end

  it "parses every key" do
    s = described_class.parse(base.merge(
      "url" => "nats://x:1", "name" => "n", "connect_timeout" => "1",
      "request_timeout" => "0.25", "concurrency" => "3"
    ), require_deployment_group: true)
    expect([s.url, s.name, s.connect_timeout, s.request_timeout, s.concurrency]).to eq(["nats://x:1", "n", 1.0, 0.25, 3])
    expect(s.connect_options).to eq(servers: ["nats://x:1"], connect_timeout: 1.0, name: "n")
  end

  it "requires the deployment group for a runtime" do
    expect { described_class.parse({}, require_deployment_group: true) }.to raise_error(ServiceMeshNats::NoDeploymentGroup)
    expect { described_class.parse({"deployment_group" => ""}, require_deployment_group: true) }.to raise_error(ServiceMeshNats::NoDeploymentGroup)
  end

  it "does not require the deployment group for a client" do
    expect(described_class.parse({}, require_deployment_group: false).deployment_group).to eq("")
  end

  {
    "non-numeric duration" => {"request_timeout" => "soon"},
    "zero duration" => {"connect_timeout" => "0"},
    "negative duration" => {"request_timeout" => "-1"},
    "non-numeric concurrency" => {"concurrency" => "many"},
    "zero concurrency" => {"concurrency" => "0"},
    "fractional concurrency" => {"concurrency" => "1.5"}
  }.each do |name, bad|
    it "rejects #{name}" do
      expect { described_class.parse(base.merge(bad), require_deployment_group: true) }.to raise_error(ServiceMeshNats::BadConfig)
    end
  end
end
