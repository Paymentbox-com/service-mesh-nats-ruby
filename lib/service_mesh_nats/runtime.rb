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

    # The client this runtime was built from, in every state.
    attr_reader :client

    # Reads ServiceMesh::DEPLOYMENT_GROUP_KEY and CONCURRENCY_KEY from +config+;
    # connection keys are ignored, the client carries them. Raises
    # ServiceMesh::NoDeploymentGroup, BadConfig, ServiceMesh::KindMismatch, or
    # ServiceMesh::InvalidTarget. Two bindings on one subject become two
    # subscriptions. The connection is not touched until start.
    def initialize(client, config, endpoints: [], subscribers: [], logger: Logger.new($stderr))
      @settings = RuntimeSettings.parse(config)
      @client = client
      @logger = logger
      @bindings = bind_all(endpoints, subscribers)

      @lock = Mutex.new
      @state = :created
      @subs = []
      @pool = nil
    end

    def running?
      @state == :running
    end

    # The client's service map.
    def service_map
      @client.service_map
    end

    # Subscribes every binding on the client's connection and begins
    # receiving. Raises AlreadyStarted on a running runtime, Stopped after
    # stop, and Closed when the client has been closed. A failure while
    # subscribing closes the client, marks the runtime stopped, and passes
    # through.
    def start
      @lock.synchronize do
        raise AlreadyStarted if @state == :running
        raise Stopped if @state == :stopped

        nc = @client.connection

        pool = Concurrent::FixedThreadPool.new(@settings.concurrency, name: "service_mesh_nats")
        subs = []
        begin
          @bindings.each do |b|
            opts = b.queue ? {queue: b.queue} : {}
            subs << nc.subscribe(b.subject, opts) { |msg| dispatch(pool, nc, b, msg) }
          end
          # Ensure the server has every subscription before start returns.
          nc.flush(FLUSH_BUDGET)
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
    # returns true and leaves the client as it is. A client the caller closed
    # directly is treated as gone: nothing is flushed and the drain result is
    # still returned.
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
      bind = lambda do |target, want, use, metadata, handler, replies|
        Subject.check_kind!(target, want, use)
        subject = Subject.format(target)
        raise ArgumentError, "#{use} #{subject} handler must respond to call" unless handler.respond_to?(:call)

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

      unless out.is_a?(ServiceMesh::Message)
        raise TypeError, "endpoint #{binding.subject} handler returned #{out.class}, expected ServiceMesh::Message"
      end

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
