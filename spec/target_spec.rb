# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Target do
  def target(*segments)
    ServiceMesh::Target.new(segments: segments, kind: :route)
  end

  describe ".subject" do
    {
      "several segments" => [%w[orders create], "orders.create"],
      "single segment" => [%w[ping], "ping"],
      "unicode printable" => [%w[héllo], "héllo"]
    }.each do |name, (segments, want)|
      it "joins #{name} with dots" do
        expect(described_class.subject(target(*segments))).to eq(want)
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
        expect { described_class.subject(target(*segments)) }.to raise_error(ServiceMesh::InvalidTarget)
      end
    end
  end

  describe ".consumer_group" do
    it "joins the deployment group when the group is nil" do
      expect(described_class.consumer_group(nil, "billing")).to eq("billing")
    end

    it "joins the deployment group when the group is empty" do
      expect(described_class.consumer_group("", "billing")).to eq("billing")
    end

    it "gives no group for none" do
      expect(described_class.consumer_group("none", "billing")).to be_nil
    end

    it "uses a named group" do
      expect(described_class.consumer_group("audit", "billing")).to eq("audit")
    end
  end
end
