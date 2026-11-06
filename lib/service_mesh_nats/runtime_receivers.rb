# frozen_string_literal: true

module ServiceMeshNats
  class Runtime
    private

    # Checks the endpoint subjects against streams, then subscribes each core
    # endpoint and subscriber and opens a pull loop for each JetStream
    # subscriber. Each is added to +nats_subscriptions+ or +pull_loops+ as it
    # opens, so abort_start can undo what opened before a failure.
    def open_receivers(nats_subscriptions, pull_loops)
      JetStream.check_endpoint_streams(@client.jetstream, @subscriptions)
      @subscriptions.each do |subscription|
        if subscription.jetstream
          pull_loops[subscription] = consume(subscription)
        else
          nats_subscriptions << subscribe(subscription)
        end
      end
    end

    # A core subscription for +subscription+, plain or in its queue group.
    def subscribe(subscription)
      pool = @pool
      options = subscription.queue ? {queue: subscription.queue} : {}
      @connection.subscribe(subscription.subject, options) do |msg|
        @callbacks_running.increment
        arrival = Time.now
        dispatch(pool) { serve(subscription, msg, arrival) }
      ensure
        @callbacks_running.decrement
      end
    end

    # Returns true once every subscription has handed its waiting messages to
    # the pool, or the connection has closed, and false when +deadline+
    # passes first. nats-pure holds a subscription's waiting messages in its
    # pending_queue. The check must hold twice in a row, because nats-pure
    # takes a message off the queue just before the callback starts.
    def wait_subscriptions_drained(nats_subscriptions, deadline)
      drained = lambda do
        @connection.closed? ||
          (nats_subscriptions.all? { |nats_subscription| nats_subscription.pending_queue.empty? } &&
            @callbacks_running.value.zero?)
      end
      loop do
        if drained.call
          sleep DRAIN_POLL
          return true if drained.call
        end
        return false if monotonic >= deadline

        sleep DRAIN_POLL
      end
    end

    # Undoes a start that failed after +nats_subscriptions+ and +pull_loops+
    # opened.
    def abort_start(nats_subscriptions, pull_loops)
      pull_loops.each_value(&:stop)
      nats_subscriptions.each { |nats_subscription| nats_subscription.unsubscribe rescue nil } # rubocop:disable Style/RescueModifier
      @pool.kill
      self.accepting = false
    end
  end
end
