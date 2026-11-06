# frozen_string_literal: true

require "concurrent"
require "logger"
require "nats/io/client"

module ServiceMeshNats
  # The service process: subscribes endpoints and subscribers on one NATS
  # connection and runs their handlers on a bounded thread pool. At most
  # CONCURRENCY_KEY handlers run at once across every endpoint and
  # subscriber, and a message waits for one of those permits before its
  # handler is posted to the pool.
  class Runtime
    # A validated Endpoint or Subscriber. queue nil means a plain subscription.
    # jetstream is a Subscriber's JetStreamSettings, and nil for an Endpoint
    # or a Subscriber without STREAM_KEY.
    Subscription = Data.define(:subject, :queue, :target, :handler, :replies, :jetstream)

    FLUSH_BUDGET = 5.0
    PERMIT_POLL = 0.1
    DRAIN_POLL = 0.05

    # Reads ServiceMesh::DEPLOYMENT_GROUP_KEY, CONCURRENCY_KEY,
    # MAX_DELIVER_KEY, PULL_EXPIRY_KEY, and PULL_HEARTBEAT_KEY from +config+.
    # Connection keys are ignored, because the client carries them. Each
    # Endpoint's and Subscriber's settings are its metadata over its Target's
    # metadata. Raises ServiceMesh::NoDeploymentGroup, BadConfig,
    # ServiceMesh::KindMismatch, or ServiceMesh::InvalidTarget. Two endpoints
    # or subscribers on one subject become two subscriptions. The connection
    # is not touched until start.
    def initialize(client, config, endpoints: [], subscribers: [], logger: Logger.new($stderr))
      @settings = RuntimeSettings.parse(config)
      @client = client
      @logger = logger
      @permits = Concurrent::Semaphore.new(@settings.concurrency)
      @pending = {}
      @pending_lock = Mutex.new
      @callbacks_running = Concurrent::AtomicFixnum.new(0)

      @subscriptions = []
      endpoints.each do |endpoint|
        subscription = Target.prepare_subscription(endpoint.target, :route, "Endpoint", endpoint.consumer_group,
          @settings.deployment_group)
        unless endpoint.handler.respond_to?(:call)
          raise ArgumentError, "Endpoint #{subscription.subject} handler must respond to call"
        end
        stream = Target.merge_settings(endpoint.target, endpoint.metadata)[STREAM_KEY].to_s
        unless stream.empty?
          raise BadConfig, "#{STREAM_KEY}=#{stream.inspect} on endpoint #{subscription.subject} applies to subscribers only"
        end
        @subscriptions << subscription.with(handler: endpoint.handler)
      end
      subscribers.each do |subscriber|
        subscription = Target.prepare_subscription(subscriber.target, :topic, "Subscriber", subscriber.consumer_group,
          @settings.deployment_group)
        unless subscriber.handler.respond_to?(:call)
          raise ArgumentError, "Subscriber #{subscription.subject} handler must respond to call"
        end
        settings = Target.merge_settings(subscriber.target, subscriber.metadata)
        jetstream = JetStreamSettings.parse(settings, subscription.subject, subscription.queue, @settings)
        @subscriptions << subscription.with(handler: subscriber.handler, jetstream: jetstream)
      end

      @lock = Mutex.new
      @state = :created
      @accepting = false
      @accept_lock = Mutex.new
      @nats_subscriptions = []
      @pull_loops = {}
      # Set by each start. Each receiver keeps the pool of the start that
      # opened it, so one left over from a failed start runs no handlers.
      @connection = nil
      @pool = nil
    end

    def running?
      @state == :running
    end

    # Subscribes every endpoint and subscriber on the client's connection and
    # begins receiving. Raises AlreadyStarted on a running runtime, Stopped
    # after stop, and Closed when the client has been closed. A failure while
    # subscribing or flushing unsubscribes what was subscribed, leaves the
    # runtime and the client as they were, and passes through.
    #
    # A Subscriber with STREAM_KEY consumes from a JetStream consumer in place
    # of a subscription. Its stream must exist. A named consumer group's
    # consumer is created or updated under the group's name, or looked up
    # when CREATE_CONSUMER_KEY is "false". A consumer group of
    # ServiceMesh::CONSUMER_GROUP_NONE gets a new consumer at each start.
    # nats-pure's errors from these calls pass through unchanged. A
    # PULL_EXPIRY_KEY or PULL_HEARTBEAT_KEY outside its limits raises
    # BadConfig.
    #
    # Before subscribing, start raises StreamCoversRoute when a stream covers
    # an endpoint subject. A server without JetStream passes the check.
    def start
      @lock.synchronize do
        check_startable
        @connection = @client.connection
        @pool = Concurrent::FixedThreadPool.new(@settings.concurrency, name: "service_mesh_nats")
        self.accepting = true

        nats_subscriptions = []
        pull_loops = {}
        begin
          open_receivers(nats_subscriptions, pull_loops)
          # Ensure the server has every subscription before start returns.
          @connection.flush(FLUSH_BUDGET)
          pull_loops.each_value(&:start)
        rescue => error
          abort_start(nats_subscriptions, pull_loops)
          raise error
        end
        # nats-pure keeps one reconnect callback, so this replaces any other.
        @connection.on_reconnect { pull_loops.each_value(&:reset) } unless pull_loops.empty?

        @nats_subscriptions = nats_subscriptions
        @pull_loops = pull_loops
        @state = :running
      end
      nil
    end

    # Drains every subscription and pull loop, waits up to +drain+ seconds
    # for in-flight handlers, flushes, and closes the client. Returns true
    # when every handler finished, false when some were abandoned. A runtime
    # that is not running returns true and leaves the client as it is. A
    # client the caller closed directly is treated as gone: nothing is
    # flushed and the drain result is still returned.
    #
    # Draining stops new deliveries and hands the messages already received
    # to their handlers. When the drain ends first, every message from an
    # acking consumer that is not yet settled is nacked and logged as a
    # warning, whether its handler had not started or was still running. A
    # handler that is still running keeps its thread, and its result is not
    # settled. Core messages still waiting are dropped.
    def stop(drain)
      drain = Float(drain)
      raise ArgumentError, "drain must be non-negative" if drain.negative?

      @lock.synchronize do
        return true unless @state == :running

        @state = :stopped
        nats_subscriptions = @nats_subscriptions
        pull_loops = @pull_loops
        # nats-pure has no public drain for one subscription. Unsubscribing
        # stops the server sending, and the messages nats-pure already holds
        # still reach the callback.
        nats_subscriptions.each { |nats_subscription| nats_subscription.unsubscribe rescue nil } # rubocop:disable Style/RescueModifier
        pull_loops.each_value(&:drain)

        deadline = monotonic + drain
        finished = wait_subscriptions_drained(nats_subscriptions, deadline) &&
          wait_consumers_closed(pull_loops, deadline)
        if finished
          self.accepting = false
          @pool.shutdown
          finished = @pool.wait_for_termination([deadline - monotonic, 0].max)
        end

        if finished
          flush_on_stop
        else
          abandon_drain(pull_loops)
          @pool.shutdown
        end
        pull_loops.each_value(&:stop)
        @client.close
        @nats_subscriptions = []
        @pull_loops = {}
        finished
      end
    end

    private

    def check_startable
      raise AlreadyStarted if @state == :running
      raise Stopped if @state == :stopped
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def flush_on_stop
      @client.connection.flush(FLUSH_BUDGET)
    rescue Closed
      # The caller closed the client directly; there is nothing to flush.
    rescue => error
      @logger.warn(Log.line("flush during stop failed", error: error.message))
    end
  end
end
