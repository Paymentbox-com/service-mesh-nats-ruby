# frozen_string_literal: true

RSpec.describe ServiceMeshNats::ClientSettings do
  it "applies defaults" do
    settings = described_class.parse({})
    expect(settings.url).to eq("nats://127.0.0.1:4222")
    expect(settings.name).to be_nil
    expect(settings.connect_timeout).to eq(5.0)
    expect(settings.request_timeout).to eq(30.0)
    expect(settings.connect_options).to eq(servers: ["nats://127.0.0.1:4222"], connect_timeout: 5.0)
  end

  it "parses every key" do
    settings = described_class.parse("url" => "nats://x:1", "name" => "n", "connect_timeout" => "1", "request_timeout" => "0.25")
    expect([settings.url, settings.name, settings.connect_timeout, settings.request_timeout]).to eq(["nats://x:1", "n", 1.0, 0.25])
    expect(settings.connect_options).to eq(servers: ["nats://x:1"], connect_timeout: 1.0, name: "n")
  end

  it "treats an empty url as absent" do
    expect(described_class.parse("url" => "").url).to eq("nats://127.0.0.1:4222")
  end

  it "rejects a non-numeric duration" do
    expect { described_class.parse("request_timeout" => "soon") }.to raise_error(ServiceMeshNats::BadConfig)
  end

  it "rejects a zero duration" do
    expect { described_class.parse("connect_timeout" => "0") }.to raise_error(ServiceMeshNats::BadConfig)
  end

  it "rejects a negative duration" do
    expect { described_class.parse("request_timeout" => "-1") }.to raise_error(ServiceMeshNats::BadConfig)
  end
end

RSpec.describe ServiceMeshNats::RuntimeSettings do
  it "applies defaults" do
    settings = described_class.parse("deployment_group" => "d")
    expect(settings.deployment_group).to eq("d")
    expect(settings.concurrency).to eq(Etc.nprocessors)
  end

  it "parses concurrency" do
    expect(described_class.parse("deployment_group" => "d", "concurrency" => "3").concurrency).to eq(3)
  end

  it "requires the deployment group" do
    expect { described_class.parse({}) }.to raise_error(ServiceMesh::NoDeploymentGroup)
  end

  it "rejects an empty deployment group" do
    expect { described_class.parse("deployment_group" => "") }.to raise_error(ServiceMesh::NoDeploymentGroup)
  end

  it "ignores connection keys" do
    settings = described_class.parse("deployment_group" => "d", "url" => "nats://x:1", "connect_timeout" => "later")
    expect(settings.deployment_group).to eq("d")
  end

  it "rejects non-numeric concurrency" do
    expect { described_class.parse("deployment_group" => "d", "concurrency" => "many") }.to raise_error(ServiceMeshNats::BadConfig)
  end

  it "rejects zero concurrency" do
    expect { described_class.parse("deployment_group" => "d", "concurrency" => "0") }.to raise_error(ServiceMeshNats::BadConfig)
  end

  it "rejects fractional concurrency" do
    expect { described_class.parse("deployment_group" => "d", "concurrency" => "1.5") }.to raise_error(ServiceMeshNats::BadConfig)
  end
end
