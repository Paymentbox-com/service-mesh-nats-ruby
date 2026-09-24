# frozen_string_literal: true

require "nats/io/client"

module ServiceMeshNats
  # Owns one NATS connection and sends messages over it. A Runtime is built
  # from a client and subscribes through its connection.
  class Client
    # The map this client was built with. The client does not otherwise use
    # it.
    attr_reader :service_map

    # Parses the connection keys of +config+ (every other key is ignored) and
    # opens the connection. A connection failure passes through unchanged.
    # Connection errors reported by nats-pure after that are logged to
    # +logger+ when one is given.
    def initialize(config, service_map, logger: nil)
      @settings = ClientSettings.parse(config)
      @service_map = service_map
      @lock = Mutex.new
      @closed = false
      @nc = NATS::IO::Client.new
      @nc.on_error { |e| logger.warn("service_mesh_nats: #{e.class}: #{e.message}") } if logger
      @nc.connect(@settings.connect_options)
    end

    # Closes the connection and marks the client closed. Returns nil.
    # Idempotent.
    def close
      @lock.synchronize do
        return nil if @closed

        @nc.close
        @closed = true
      end
      nil
    end

    # The NATS::IO::Client this client owns. Used by Runtime. Raises Closed
    # after close.
    def connection
      raise Closed if @closed

      @nc
    end

    # Sends +message+ to a route target and returns the reply.
    #
    # +opts+ may carry REQUEST_TIMEOUT_KEY to override the configured request
    # timeout for this call. Expiry raises NATS::Timeout; no receiver raises
    # NATS::IO::NoRespondersError.
    def request(message, opts = {})
      Subject.check_kind!(message.target, :route, "request")
      subject = Subject.format(message.target)
      timeout = Settings.duration(opts.to_h, REQUEST_TIMEOUT_KEY, @settings.request_timeout)
      nc = connection

      reply = nc.request_msg(nats_msg(subject, message), timeout: timeout)
      header = reply.header || {}
      raise HandlerError.new(header[HANDLER_ERROR_HEADER].to_s) if header.key?(HANDLER_ERROR_HEADER)

      ServiceMesh::Message.new(target: message.target, metadata: header, payload: reply.data.to_s)
    end

    # Sends +message+ to a topic target. +opts+ is accepted for the interface
    # and not read.
    def publish(message, opts = {})
      Subject.check_kind!(message.target, :topic, "publish")
      subject = Subject.format(message.target)
      connection.publish_msg(nats_msg(subject, message))
      nil
    end

    private

    def nats_msg(subject, message)
      header = message.metadata.empty? ? nil : message.metadata.transform_keys(&:to_s).transform_values(&:to_s)
      NATS::Msg.new(subject: subject, data: message.payload, header: header)
    end
  end
end
