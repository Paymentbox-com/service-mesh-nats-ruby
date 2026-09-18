# frozen_string_literal: true

module ServiceMeshNats
  class Error < StandardError; end

  # Contract errors, raised for misuse of the specification.
  class KindMismatch < Error; end

  class InvalidTarget < Error; end

  class NoDeploymentGroup < Error
    def initialize(msg = "config #{DEPLOYMENT_GROUP_KEY} is required") = super
  end

  # Runtime-specific errors.
  class BadConfig < Error; end

  class NotRunning < Error
    def initialize(msg = "runtime is not running") = super
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
