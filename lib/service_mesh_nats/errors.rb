# frozen_string_literal: true

module ServiceMeshNats
  # This transport's errors. The contract errors are ServiceMesh::KindMismatch,
  # ServiceMesh::InvalidTarget, and ServiceMesh::NoDeploymentGroup.
  class Error < StandardError; end

  class BadConfig < Error; end

  class NotConnected < Error
    def initialize(msg = "client is not connected") = super
  end

  class Closed < Error
    def initialize(msg = "client has been closed") = super
  end

  class AlreadyStarted < Error
    def initialize(msg = "runtime already started") = super
  end

  class Stopped < Error
    def initialize(msg = "runtime has been stopped; it is not restartable") = super
  end

  class DuplicateTarget < Error; end

  # Raised by Client#request when the serving handler raised. +text+ is the
  # handler's error message as sent by the serving runtime.
  class HandlerError < Error
    attr_reader :text

    def initialize(text)
      @text = text
      super("handler failed: #{text}")
    end
  end

  # Reply header set when an endpoint handler fails. Application metadata
  # should not use this key.
  HANDLER_ERROR_HEADER = "Mesh-Handler-Error"
end
