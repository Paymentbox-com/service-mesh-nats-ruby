# frozen_string_literal: true

module ServiceMeshNats
  # This transport's errors. The contract errors are ServiceMesh::KindMismatch,
  # ServiceMesh::InvalidTarget, and ServiceMesh::NoDeploymentGroup.
  class Error < StandardError; end

  class BadConfig < Error; end

  class Closed < Error
    def initialize(msg = "client has been closed") = super
  end

  class AlreadyStarted < Error
    def initialize(msg = "runtime already started") = super
  end

  class Stopped < Error
    def initialize(msg = "runtime has been stopped; it is not restartable") = super
  end

  # Raised by Runtime#start when a JetStream stream covers one of the
  # runtime's endpoint subjects, because the stream would answer requests to
  # it. +subject+ is the endpoint subject, and +stream+ the stream's name.
  class StreamCoversRoute < Error
    attr_reader :subject, :stream

    def initialize(subject, stream)
      @subject = subject
      @stream = stream
      super("a stream covers an endpoint subject: subject #{subject.inspect}, stream #{stream.inspect}")
    end
  end

  # Raised by Client#request when the serving handler raised. +text+ is the
  # handler's error message as sent by the serving runtime.
  class HandlerError < Error
    attr_reader :text

    def initialize(text)
      @text = text
      super("handler failed: #{text}")
    end
  end
end
