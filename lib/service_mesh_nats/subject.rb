# frozen_string_literal: true

module ServiceMeshNats
  # Assembles targets into NATS subjects and resolves queue groups.
  module Subject
    module_function

    # Joins segments with "." or raises InvalidTarget with the reason.
    def format(target)
      raise ServiceMesh::InvalidTarget, "no segments" if target.segments.empty?

      target.segments.each_with_index do |seg, i|
        reason = segment_problem(seg)
        raise ServiceMesh::InvalidTarget, "segment #{i} #{seg.inspect}: #{reason}" if reason
      end
      target.segments.join(".")
    end

    def segment_problem(seg)
      return "empty" if seg.empty?
      return "contains '.'" if seg.include?(".")
      return "contains wildcard" if seg.include?("*") || seg.include?(">")
      return "contains whitespace" if seg.match?(/\s/)
      return "contains non-printable character" if seg.match?(/[^[:print:]]/)

      nil
    end

    # Raises KindMismatch unless the target's kind is +want+.
    def check_kind!(target, want, use)
      return if target.kind == want

      raise ServiceMesh::KindMismatch, "#{use} requires a #{want} target, got #{target.kind}"
    end

    # The queue group for a binding. Binding metadata wins, then target
    # metadata, then the deployment group. nil means a plain subscription.
    def consumer_group(binding_metadata, target_metadata, deployment_group)
      [binding_metadata, target_metadata].each do |md|
        next unless md.key?(ServiceMesh::CONSUMER_GROUP_KEY)

        value = md[ServiceMesh::CONSUMER_GROUP_KEY].to_s
        next if value.empty?
        return nil if value == ServiceMesh::CONSUMER_GROUP_NONE

        return value
      end
      deployment_group
    end
  end
end
