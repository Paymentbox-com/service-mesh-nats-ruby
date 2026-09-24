# frozen_string_literal: true

require "nats/io/client"

module ServiceMeshNats
  # Owns one NATS connection and sends messages over it. A Runtime builds its
  # client with +connect: false+, connects it in start, and closes it in stop.
  class Client
    # The map this client was built with: the one given to +new+, or the
    # runtime's for a client from Runtime#client. The client does not
    # otherwise use it.
    attr_reader :service_map

    # Parses +config+ (ServiceMesh::DEPLOYMENT_GROUP_KEY is ignored), builds
    # the NATS connection, and opens it unless +connect+ is false. Connection
    # errors reported by nats-pure are logged to +logger+ when one is given.
    def initialize(config, service_map, logger: nil, connect: true)
      @settings = Settings.parse(config, require_deployment_group: false)
      @service_map = service_map
      @logger = logger
      @lock = Mutex.new
      @state = :disconnected
      @nc = build_nc
      self.connect if connect
    end

    # Opens the connection. Returns nil. A connected client returns without
    # effect; a closed client raises Closed. A connection failure passes
    # through unchanged and leaves the client not connected.
    def connect
      @lock.synchronize do
        raise Closed if @state == :closed
        return nil if @state == :connected

        begin
          @nc.connect(@settings.connect_options)
        rescue => e
          # nats-pure remembers that connect was called, so a retry needs a
          # fresh instance.
          @nc = build_nc
          raise e
        end
        @state = :connected
      end
      nil
    end

    # Closes the connection if one is open and marks the client closed.
    # Returns nil. Idempotent.
    def close
      @lock.synchronize do
        return nil if @state == :closed

        @nc.close if @state == :connected
        @state = :closed
      end
      nil
    end

    # The NATS::IO::Client this client owns. Used by Runtime. Raises
    # NotConnected before connect and Closed after close.
    def connection
      case @state
      when :connected then @nc
      when :closed then raise Closed
      else raise NotConnected
      end
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

    def build_nc
      nc = NATS::IO::Client.new
      nc.on_error { |e| @logger.warn("service_mesh_nats: #{e.class}: #{e.message}") } if @logger
      nc
    end

    def nats_msg(subject, message)
      header = message.metadata.empty? ? nil : message.metadata.transform_keys(&:to_s).transform_values(&:to_s)
      NATS::Msg.new(subject: subject, data: message.payload, header: header)
    end
  end
end
