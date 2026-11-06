# frozen_string_literal: true

module ServiceMeshNats
  # A JetStream subscriber's consumer as the server describes it. acks is
  # false for a consumer that acks nothing. ack_wait is seconds, and backoff
  # is a list of seconds or nil. max_deliver is -1 when deliveries are
  # unlimited.
  Consumer = Data.define(:stream, :name, :acks, :ack_wait, :backoff, :max_deliver) do
    # When the server sends a delivery again unless it is acked: its arrival
    # plus its backoff entry when the consumer has backoff, and the
    # consumer's ack_wait otherwise. Deliveries past the end of the backoff
    # list use its last entry.
    def deadline(arrival, delivered)
      return arrival + ack_wait unless backoff&.any?

      arrival + backoff[[delivered - 1, backoff.length - 1].min]
    end

    # Whether delivery number +delivered+ is the last one the server sends.
    # A max_deliver of -1 means no limit, so no delivery is the last.
    def last_delivery?(delivered)
      max_deliver.positive? && delivered >= max_deliver
    end
  end
  private_constant :Consumer

  class Runtime
    private

    # Looks up the subscriber's stream, then creates, updates, or looks up
    # its consumer. Returns the Consumer the server reports. nats-pure's
    # errors pass through unchanged.
    def open_consumer(settings)
      @client.jetstream.stream_info(settings.stream)
      if settings.group.nil?
        ConsumerAPI.create(@connection, settings.stream, NATS::NUID.next, settings.consumer_config, update: false)
      elsif settings.create
        ConsumerAPI.create(@connection, settings.stream, settings.group, settings.consumer_config, update: true)
      else
        ConsumerAPI.info(@connection, settings.stream, settings.group)
      end
    end

    # Opens the JetStream subscriber's consumer and a PullLoop that hands
    # each message it takes a permit for to receive. Raises BadConfig for
    # pull settings outside their limits, before the consumer is opened.
    def consume(subscription)
      settings = subscription.jetstream
      settings.check_pull_limits!
      consumer = open_consumer(settings)
      pool = @pool
      pull_loop = nil
      pull_loop = PullLoop.new(connection: @connection, subject: subscription.subject, settings: settings,
        consumer: consumer, concurrency: @settings.concurrency, permits: @permits, logger: @logger) do |msg, arrival|
        receive(pool, subscription, pull_loop, msg, arrival)
      end
    end

    # Returns true once every pull loop has handed over the messages it
    # received, and false when +deadline+ passes first.
    def wait_consumers_closed(pull_loops, deadline)
      until pull_loops.each_value.all?(&:drained?)
        return false if monotonic >= deadline

        sleep DRAIN_POLL
      end
      true
    end

    # Whether deliveries for +subscription+ from +consumer+ are acked.
    def acks?(subscription, consumer)
      subscription.jetstream.ack_policy != :none && consumer.acks
    end
  end
end
