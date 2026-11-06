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
      @connection = NATS::IO::Client.new
      if logger
        @connection.on_error { |error| logger.warn(Log.line("connection error", error: "#{error.class}: #{error.message}")) }
      end
      @connection.connect(@settings.connect_options)
      @jetstream = @connection.jetstream
    end

    # Closes the connection and marks the client closed. Returns nil.
    # Idempotent.
    def close
      @lock.synchronize do
        return nil if @closed

        @connection.close
        @closed = true
      end
      nil
    end

    # The NATS::IO::Client this client owns. Raises Closed after close.
    def connection
      raise Closed if @closed

      @connection
    end

    # The NATS::JetStream context this client publishes with. Raises Closed
    # after close.
    def jetstream
      raise Closed if @closed

      @jetstream
    end

    # The NATS::JetStream::API::StreamInfo of the stream named by +target+'s
    # STREAM_KEY metadata, or nil when it has none, which is so for every
    # route target. It names the stream a publish to +target+ uses when no
    # per-call option overrides it. Raises Closed after close, and nats-pure's
    # error unchanged when the lookup fails.
    def stream_info(target)
      name = target.metadata.to_h[STREAM_KEY].to_s
      return nil if name.empty?

      jetstream.stream_info(name)
    end

    # The configured REQUEST_TIMEOUT_KEY in seconds. The runtime bounds the
    # wait for a double ack's confirmation with it.
    def request_timeout
      @settings.request_timeout
    end

    # Sends +message+ to a route target and returns the reply.
    #
    # Settings are read from +options+, then the target's metadata, then the
    # client's config. REQUEST_TIMEOUT_KEY bounds the call. STREAM_KEY raises
    # BadConfig. Expiry raises NATS::Timeout, and no receiver raises
    # NATS::IO::NoRespondersError.
    #
    # The request carries ServiceMesh::TIMEOUT_KEY, set to the request timeout
    # in whole milliseconds. ServiceMesh::MESSAGE_ID_KEY is copied to
    # Nats-Msg-Id.
    def request(message, options = {})
      Target.check_kind!(message.target, :route, "request")
      subject = Target.subject(message.target)
      settings = Target.merge_settings(message.target, options)
      unless settings[STREAM_KEY].to_s.empty?
        raise BadConfig, "#{STREAM_KEY}=#{settings[STREAM_KEY].inspect} applies to topic targets only"
      end
      timeout = Config.duration(settings, REQUEST_TIMEOUT_KEY, @settings.request_timeout)
      nats_connection = connection

      msg = nats_msg(subject, message)
      msg.header = (msg.header || {}).merge(ServiceMesh::TIMEOUT_KEY => (timeout * 1000).floor.to_s)
      reply = nats_connection.request_msg(msg, timeout: timeout)
      header = reply.header || {}
      raise HandlerError.new(header[ServiceMesh::HANDLER_ERROR_KEY].to_s) if header.key?(ServiceMesh::HANDLER_ERROR_KEY)

      ServiceMesh::Message.new(target: message.target, metadata: header, payload: reply.data.to_s)
    end

    # Sends +message+ to a topic target. ServiceMesh::MESSAGE_ID_KEY is copied
    # to Nats-Msg-Id.
    #
    # Settings are read from +options+, then the target's metadata, then the
    # client's config. When they carry STREAM_KEY, the message goes out as a
    # JetStream publish that expects that stream, and publish waits for the
    # stream's ack, bounded by REQUEST_TIMEOUT_KEY. nats-pure's errors pass
    # through unchanged: NATS::Timeout on expiry,
    # NATS::JetStream::Error::NoStreamResponse when no stream takes the
    # subject, and NATS::JetStream::Error::APIError when the stream refuses the
    # message.
    def publish(message, options = {})
      Target.check_kind!(message.target, :topic, "publish")
      subject = Target.subject(message.target)
      settings = Target.merge_settings(message.target, options)
      stream = settings[STREAM_KEY].to_s
      msg = nats_msg(subject, message)
      if stream.empty?
        connection.publish_msg(msg)
        return nil
      end

      timeout = Config.duration(settings, REQUEST_TIMEOUT_KEY, @settings.request_timeout)
      jetstream.publish(subject, msg.data, header: msg.header, stream: stream, timeout: timeout)
      nil
    end

    private

    # The NATS message for +message+, with its metadata as the header.
    def nats_msg(subject, message)
      NATS::Msg.new(subject: subject, data: message.payload, header: Header.outgoing_header(message.metadata))
    end
  end
end
