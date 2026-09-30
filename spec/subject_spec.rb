# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Subject do
  def target(*segments)
    ServiceMesh::Target.new(segments: segments, kind: :route)
  end

  describe ".format" do
    {
      "several segments" => [%w[orders create], "orders.create"],
      "single segment" => [%w[ping], "ping"],
      "unicode printable" => [%w[héllo], "héllo"]
    }.each do |name, (segments, want)|
      it "joins #{name} with dots" do
        expect(described_class.format(target(*segments))).to eq(want)
      end
    end

    {
      "no segments" => [],
      "empty segment" => ["a", ""],
      "dot in segment" => ["a.b"],
      "star wildcard" => ["*"],
      "gt wildcard" => ["a", ">"],
      "whitespace" => ["a b"],
      "non-printable" => ["a\x01"]
    }.each do |name, segments|
      it "rejects #{name}" do
        expect { described_class.format(target(*segments)) }.to raise_error(ServiceMesh::InvalidTarget)
      end
    end
  end

  describe ".consumer_group" do
    none = {"consumer_group" => "none"}
    named = ->(n) { {"consumer_group" => n} }

    [
      ["absent joins the deployment group", {}, "billing"],
      ["none gives no group", none, nil],
      ["a name is the group", named["audit"], "audit"],
      ["empty falls through to the deployment group", named[""], "billing"]
    ].each do |name, metadata, want|
      it name do
        expect(described_class.consumer_group(metadata, "billing")).to eq(want)
      end
    end
  end
end
