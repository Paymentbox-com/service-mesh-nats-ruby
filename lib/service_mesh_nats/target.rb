# frozen_string_literal: true

module ServiceMeshNats
  # Assembles targets into NATS subjects, resolves queue groups, and merges
  # settings over a target's metadata.
  module Target
    # Joins segments with "." or raises InvalidTarget with the reason.
    def self.subject(target)
      raise ServiceMesh::InvalidTarget, "no segments" if target.segments.empty?

      target.segments.each_with_index do |segment, index|
        reason = segment_problem(segment)
        raise ServiceMesh::InvalidTarget, "segment #{index} #{segment.inspect}: #{reason}" if reason
      end
      target.segments.join(".")
    end

    def self.segment_problem(segment)
      return "empty" if segment.empty?
      return "contains '.'" if segment.include?(".")
      return "contains wildcard" if segment.include?("*") || segment.include?(">")
      return "contains whitespace" if segment.match?(/\s/)
      return "contains non-printable character" if segment.match?(/[^[:print:]]/)

      nil
    end

    # A Runtime::Subscription for an Endpoint's or Subscriber's +target+, with
    # its subject and queue group, and without its handler or JetStream
    # settings. Raises ServiceMesh::KindMismatch unless the target's kind is
    # +kind+, and ServiceMesh::InvalidTarget for a target the transport cannot
    # carry. +use+ names the receiver in the error.
    def self.prepare_subscription(target, kind, use, group, deployment_group)
      check_kind!(target, kind, use)
      Runtime::Subscription.new(subject: subject(target), queue: consumer_group(group, deployment_group),
        target: target, handler: nil, replies: kind == :route, jetstream: nil)
    end

    # Raises KindMismatch unless the target's kind is +kind+.
    def self.check_kind!(target, kind, use)
      return if target.kind == kind

      raise ServiceMesh::KindMismatch, "#{use} requires a #{kind} target, got #{target.kind}"
    end

    # The queue group of an Endpoint or Subscriber from its consumer_group,
    # falling back to the runtime's deployment group. nil means a plain
    # subscription.
    def self.consumer_group(group, deployment_group)
      value = group.to_s
      return deployment_group if value.empty?
      return nil if value == ServiceMesh::CONSUMER_GROUP_NONE

      value
    end

    # A Target's metadata with +over+ on top, so a value in +over+ wins.
    # +over+ is a call's options or an Endpoint's or Subscriber's metadata.
    def self.merge_settings(target, over)
      target.metadata.to_h.merge(over.to_h)
    end
  end
end
