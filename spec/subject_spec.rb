# frozen_string_literal: true

RSpec.describe ServiceMeshNats::Subject do
  def target(*segments)
    ServiceMeshNats::Target.new(segments: segments, kind: :route)
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
        expect { described_class.format(target(*segments)) }.to raise_error(ServiceMeshNats::InvalidTarget)
      end
    end
  end

  describe ".consumer_group" do
    none = {"consumer_group" => "none"}
    named = ->(n) { {"consumer_group" => n} }

    [
      ["absent everywhere joins the deployment group", {}, {}, "billing"],
      ["none on binding gives no group", none, {}, nil],
      ["name on binding", named["audit"], {}, "audit"],
      ["name on target", {}, named["audit"], "audit"],
      ["none on target", {}, none, nil],
      ["binding wins over target", named["from-binding"], named["from-target"], "from-binding"],
      ["empty binding value falls through to target", named[""], named["audit"], "audit"],
      ["empty everywhere falls through to the deployment group", named[""], named[""], "billing"]
    ].each do |name, binding_md, target_md, want|
      it name do
        expect(described_class.consumer_group(binding_md, target_md, "billing")).to eq(want)
      end
    end
  end
end
