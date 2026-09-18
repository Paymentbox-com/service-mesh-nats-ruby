# frozen_string_literal: true

module ServiceMeshNats
  KINDS = %i[route topic].freeze

  # Configuration keys the specification defines. Everything else belongs to
  # the transport.
  DEPLOYMENT_GROUP_KEY = "deployment_group"
  CONSUMER_GROUP_KEY = "consumer_group"
  CONSUMER_GROUP_NONE = "none"

  # Identifies a receiving channel on the mesh. Segments are assembled into a
  # NATS subject by the runtime; the subject never leaves it.
  Target = Data.define(:segments, :kind, :metadata) do
    def initialize(segments:, kind:, metadata: {})
      raise ArgumentError, "kind must be one of #{KINDS.inspect}, got #{kind.inspect}" unless KINDS.include?(kind)

      super(segments: Array(segments).map(&:to_s).freeze, kind: kind, metadata: metadata.to_h.freeze)
    end

    # Same channel: same segments and kind. Metadata is configuration.
    def same_channel?(other)
      segments == other.segments && kind == other.kind
    end
  end

  ServiceMap = Data.define(:targets) do
    def initialize(targets: [])
      super(targets: Array(targets).freeze)
    end
  end

  # What travels between services. Payload is always BINARY encoded.
  Message = Data.define(:target, :metadata, :payload) do
    def initialize(target:, metadata: {}, payload: "")
      super(target: target, metadata: metadata.to_h.freeze, payload: payload.to_s.b.freeze)
    end
  end

  # A route target paired with a handler that returns a Message.
  Endpoint = Data.define(:target, :metadata, :handler) do
    def initialize(target:, handler:, metadata: {})
      super(target: target, metadata: metadata.to_h.freeze, handler: handler)
    end
  end

  # A topic target paired with a handler whose return value is ignored.
  Subscriber = Data.define(:target, :metadata, :handler) do
    def initialize(target:, handler:, metadata: {})
      super(target: target, metadata: metadata.to_h.freeze, handler: handler)
    end
  end
end
