# frozen_string_literal: true

module ServiceMeshNats
  class Runtime
    # The client this runtime was built from, in every state. Its request and
    # publish raise Closed after stop. Closing it directly ends the runtime's
    # connection.
    attr_reader :client

    # The client's service map. This runtime does not otherwise use it.
    def service_map
      @client.service_map
    end

    # The runtime's Receiver for +target+'s endpoint or subscriber while the
    # runtime is running, or nil when it is not running or has neither for
    # +target+. Targets are equal when their segments, kind, and metadata
    # are, so a target's kind decides which receiver it can match.
    def receiver(target)
      @lock.synchronize do
        next nil unless @state == :running

        subscription = @subscriptions.find { |candidate| candidate.target == target }
        next nil unless subscription

        Receiver.new(@client, subscription, @pull_loops[subscription])
      end
    end
  end

  # An endpoint or subscriber the runtime is running. It reads the settings
  # the runtime uses, which are the Endpoint's or Subscriber's metadata over
  # its Target's metadata.
  class Receiver
    def initialize(client, subscription, pull_loop)
      @client = client
      @subscription = subscription
      @pull_loop = pull_loop
    end

    # The receiver's target.
    def target
      @subscription.target
    end

    # The resolved consumer group: the Endpoint's or Subscriber's
    # consumer_group, or the runtime's deployment group when that is empty.
    def consumer_group
      @subscription.queue || ServiceMesh::CONSUMER_GROUP_NONE
    end

    # The NATS::JetStream::API::StreamInfo of the stream named by the
    # receiver's settings, or nil when they have no STREAM_KEY, which is so
    # for every endpoint. Raises Closed after the runtime's client is closed,
    # and nats-pure's error unchanged when the lookup fails.
    def stream_info
      return nil unless @subscription.jetstream

      @client.jetstream.stream_info(@subscription.jetstream.stream)
    end

    # The NATS::JetStream::API::ConsumerInfo of the consumer the receiver
    # pulls from at the time of the call, or nil when its settings have no
    # STREAM_KEY. An ordered consumer replaced after a gap is reported under
    # its new name. nats-pure reports ack_wait and inactive_threshold in
    # whole seconds, rounded down. Raises Closed after the runtime's client
    # is closed, and nats-pure's error unchanged when the lookup fails.
    def consumer_info
      return nil unless @pull_loop

      consumer = @pull_loop.consumer
      @client.jetstream.consumer_info(consumer.stream, consumer.name)
    end
  end
end
