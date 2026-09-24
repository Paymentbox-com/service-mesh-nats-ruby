# frozen_string_literal: true

require "concurrent"
require "logger"
require "nats/io/client"

module ServiceMeshNats
  # The service process: binds endpoints and subscribers over one NATS
  # connection and runs their handlers on a bounded thread pool.
  class Runtime
    # A validated Endpoint or Subscriber. queue nil means a plain subscription.
    Binding = Data.define(:subject, :queue, :target, :handler, :replies)

    FLUSH_BUDGET = 5.0

    attr_reader :service_map, :client

    # Raises ServiceMesh::NoDeploymentGroup, BadConfig, ServiceMesh::KindMismatch,
    # ServiceMesh::InvalidTarget, or DuplicateTarget. The client is built
    # unconnected; start connects it and stop closes it.
    def initialize(config, service_map, endpoints: [], subscribers: [], logger: Logger.new($stderr))
      @settings = Settings.parse(config, require_deployment_group: true)
      @service_map = service_map
      @logger = logger
      @client = Client.new(config, service_map, logger: logger, connect: false)
      @bindings = bind_all(endpoints, subscribers)

      @lock = Mutex.new
      @state = :created
      @subs = []
      @pool = nil
    end

    def running?
      @state == :running
    end

    # Connects the client, subscribes every binding, and begins receiving.
    # Raises AlreadyStarted on a running runtime and Stopped after stop. A
    # connection failure passes through and leaves the runtime startable. A
    # failure while subscribing closes the client, marks the runtime stopped,
    # and passes through.
    def start
      @lock.synchronize do
        raise AlreadyStarted if @state == :running
        raise Stopped if @state == :stopped

        @client.connect
        nc = @client.connection

        pool = Concurrent::FixedThreadPool.new(@settings.concurrency, name: "service_mesh_nats")
        subs = []
        begin
          @bindings.each do |b|
            opts = b.queue ? {queue: b.queue} : {}
            subs << nc.subscribe(b.subject, opts) { |msg| dispatch(pool, nc, b, msg) }
          end
          # Ensure the server has every subscription before start returns.
          nc.flush(@settings.connect_timeout)
        rescue => e
          subs.each { |s| s.unsubscribe rescue nil } # rubocop:disable Style/RescueModifier
          pool.kill
          @client.close
          @state = :stopped
          raise e
        end

        @subs = subs
        @pool = pool
        @state = :running
      end
      nil
    end

    # Stops receiving, waits up to +drain+ seconds for in-flight handlers,
    # flushes, then closes the client. Returns true when every handler
    # finished, false when some were abandoned. A runtime that is not running
    # returns true. A client the caller closed directly is treated as gone:
    # nothing is flushed and the drain result is still returned.
    def stop(drain)
      drain = Float(drain)
      raise ArgumentError, "drain must be non-negative" if drain.negative?

      @lock.synchronize do
        return true unless @state == :running

        @state = :stopped
        @subs.each { |s| s.unsubscribe rescue nil } # rubocop:disable Style/RescueModifier

        @pool.shutdown
        finished = @pool.wait_for_termination(drain)
        flush if finished
        @client.close
        @subs = []
        finished
      end
    end

    private

    def flush
      @client.connection.flush(FLUSH_BUDGET)
    rescue Closed
      # The caller closed the client directly; there is nothing to flush.
    rescue => e
      @logger.warn("service_mesh_nats: flush during stop failed: #{e.message}")
    end

    def bind_all(endpoints, subscribers)
      seen = {}
      bind = lambda do |target, want, use, metadata, handler, replies|
        Subject.check_kind!(target, want, use)
        subject = Subject.format(target)
        raise DuplicateTarget, subject if seen.key?(subject)
        raise ArgumentError, "#{use} #{subject} handler must respond to call" unless handler.respond_to?(:call)

        seen[subject] = true
        queue = Subject.consumer_group(metadata, target.metadata, @settings.deployment_group)
        Binding.new(subject: subject, queue: queue, target: target, handler: handler, replies: replies)
      end

      endpoints.map { |e| bind.call(e.target, :route, "Endpoint", e.metadata, e.handler, true) } +
        subscribers.map { |s| bind.call(s.target, :topic, "Subscriber", s.metadata, s.handler, false) }
    end

    # Runs on nats-pure's subscription thread; hands the message to the pool.
    def dispatch(pool, nc, binding, msg)
      pool.post { serve(nc, binding, msg) }
    rescue Concurrent::RejectedExecutionError
      # The pool is shutting down; the subscription is already gone.
    end

    def serve(nc, binding, msg)
      inbound = ServiceMesh::Message.new(target: binding.target, metadata: msg.header || {}, payload: msg.data.to_s)

      out = binding.handler.call(inbound)
      return unless binding.replies && msg.reply

      out = ServiceMesh::Message.new(target: binding.target) unless out.is_a?(ServiceMesh::Message)
      reply(nc, msg.reply, out.metadata, out.payload)
    rescue => e
      @logger.error("service_mesh_nats: handler failed on #{binding.subject}: #{e.class}: #{e.message}")
      reply(nc, msg.reply, {HANDLER_ERROR_HEADER => e.message}, "") if binding.replies && msg.reply
    end

    def reply(nc, subject, metadata, payload)
      header = metadata.empty? ? nil : metadata.transform_keys(&:to_s).transform_values(&:to_s)
      nc.publish_msg(NATS::Msg.new(subject: subject, data: payload.to_s.b, header: header))
    rescue => e
      @logger.error("service_mesh_nats: reply failed: #{e.class}: #{e.message}")
    end
  end
end
