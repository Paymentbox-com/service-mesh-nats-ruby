# frozen_string_literal: true

require "nats/io/client"

module ServiceMeshNats
  # Sends messages over a NATS connection.
  class Client
    # Connects and returns a client that owns its connection.
    # DEPLOYMENT_GROUP_KEY is ignored.
    def initialize(config = {})
      @settings = Settings.parse(config, require_deployment_group: false)
      @subjects = {}
      @subjects_lock = Mutex.new
      @owns_connection = true
      @nc = NATS::IO::Client.new
      @nc.connect(@settings.connect_options)
    end

    # A client whose connection a Runtime attaches and detaches. Its close is
    # a no-op.
    def self.shared(settings)
      client = allocate
      client.instance_variable_set(:@settings, settings)
      client.instance_variable_set(:@subjects, {})
      client.instance_variable_set(:@subjects_lock, Mutex.new)
      client.instance_variable_set(:@owns_connection, false)
      client.instance_variable_set(:@nc, nil)
      client
    end

    # Sends +message+ to a route target and returns the reply.
    #
    # +opts+ may carry REQUEST_TIMEOUT_KEY to override the configured request
    # timeout for this call. Expiry raises NATS::Timeout; no receiver raises
    # NATS::IO::NoRespondersError.
    def request(message, opts = {})
      Subject.check_kind!(message.target, :route, "request")
      subject = subject_for(message.target)
      timeout = Settings.duration(opts.to_h, REQUEST_TIMEOUT_KEY, @settings.request_timeout)
      nc = connection

      reply = nc.request_msg(nats_msg(subject, message), timeout: timeout)
      header = reply.header || {}
      raise HandlerError.new(header[HANDLER_ERROR_HEADER].to_s) if header.key?(HANDLER_ERROR_HEADER)

      Message.new(target: message.target, metadata: header, payload: reply.data.to_s)
    end

    # Sends +message+ to a topic target. +opts+ is accepted for the interface
    # and not read.
    def publish(message, opts = {})
      Subject.check_kind!(message.target, :topic, "publish")
      subject = subject_for(message.target)
      connection.publish_msg(nats_msg(subject, message))
      nil
    end

    # Releases the connection when this client owns it.
    def close
      return nil unless @owns_connection

      nc, @nc = @nc, nil
      nc&.close
      nil
    end

    # Used by Runtime.
    def attach(nc)
      @nc = nc
    end

    def detach
      @nc = nil
    end

    private

    def connection
      @nc or raise NotRunning
    end

    def nats_msg(subject, message)
      header = message.metadata.empty? ? nil : message.metadata.transform_keys(&:to_s).transform_values(&:to_s)
      NATS::Msg.new(subject: subject, data: message.payload, header: header)
    end

    # Formats and validates a target once per channel, then serves it from
    # cache.
    def subject_for(target)
      key = [target.kind, target.segments]
      @subjects_lock.synchronize do
        @subjects[key] ||= Subject.format(target)
      end
    end
  end
end
