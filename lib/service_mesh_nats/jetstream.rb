# frozen_string_literal: true

require "nats/io/client"
require "time"

module ServiceMeshNats
  # Metadata keys the runtime adds to each JetStream delivery it hands to a
  # handler, from the delivery's JetStream metadata.
  STREAM_METADATA_KEY = "Mesh-Nats-Stream"
  CONSUMER_METADATA_KEY = "Mesh-Nats-Consumer"
  STREAM_SEQUENCE_METADATA_KEY = "Mesh-Nats-Stream-Sequence"
  CONSUMER_SEQUENCE_METADATA_KEY = "Mesh-Nats-Consumer-Sequence"
  NUM_PENDING_METADATA_KEY = "Mesh-Nats-Num-Pending"
  TIMESTAMP_METADATA_KEY = "Mesh-Nats-Timestamp"

  # A Subscriber's JetStream settings, read when the runtime is built.
  #
  # +group+ is the consumer's name, and nil means a consumer group of
  # ServiceMesh::CONSUMER_GROUP_NONE. +ack_policy+ is :double, :explicit, or
  # :none. +nak_delay+ is seconds or nil. +consumer_config+ is what the
  # runtime sends to the server for the consumer, as
  # NATS::JetStream::API::ConsumerConfig fields with durations in seconds.
  # +pull_expiry+ and +pull_heartbeat+ are the seconds each pull request
  # carries.
  JetStreamSettings = Data.define(:stream, :group, :create, :ack_policy, :nak_delay, :consumer_config,
    :pull_expiry, :pull_heartbeat) do
    # Reads the settings from +settings+, a Subscriber's metadata over its
    # Target's. Returns nil when they set no STREAM_KEY. +runtime+ is the
    # RuntimeSettings, which give the defaults for max_deliver and the pull
    # keys.
    def self.parse(settings, subject, group, runtime)
      stream = settings[STREAM_KEY].to_s
      return nil if stream.empty?

      create = Config.choice(settings, CREATE_CONSUMER_KEY, %w[true false]) == "true"
      raise BadConfig, "#{CREATE_CONSUMER_KEY}=\"false\" needs a named consumer group" if !create && group.nil?

      ack_policy = Config.choice(settings, ACK_POLICY_KEY, %w[double explicit none]).to_sym
      expiry = Config.duration(settings, PULL_EXPIRY_KEY, runtime.pull_expiry || DEFAULT_PULL_EXPIRY)
      heartbeat = Config.duration(settings, PULL_HEARTBEAT_KEY, runtime.pull_heartbeat) ||
        default_heartbeat(expiry, ordered?(group, ack_policy))
      new(
        stream: stream,
        group: group,
        create: create,
        ack_policy: ack_policy,
        nak_delay: Config.duration(settings, NAK_DELAY_KEY, nil),
        consumer_config: consumer_config(settings, subject, group, create, ack_policy, runtime.max_deliver),
        pull_expiry: expiry,
        pull_heartbeat: heartbeat
      )
    end

    # Whether a subscriber with consumer group +group+ and +ack_policy+ uses
    # an ordered consumer.
    def self.ordered?(group, ack_policy)
      group.nil? && ack_policy == :none
    end

    # nats.go's default heartbeat: half of the expiry, capped at 30 seconds.
    # An ordered consumer's is 5 seconds, or half of the expiry when that is
    # under 10 seconds.
    def self.default_heartbeat(expiry, ordered)
      if ordered
        (expiry < 10) ? expiry / 2 : 5.0
      else
        [expiry / 2, 30.0].min
      end
    end
    private_class_method :default_heartbeat

    # The consumer settings. The ack settings are read and checked whatever
    # the ack policy, and sent only when the consumer acks. An ordered
    # consumer is kept in memory with one replica, like nats.go's, because
    # the runtime creates it again from the last stream sequence it handed
    # over when the server loses it.
    def self.consumer_config(settings, subject, group, create, ack_policy, max_deliver)
      ordered = ordered?(group, ack_policy)
      default_inactive = if ordered
        ORDERED_INACTIVE_THRESHOLD
      elsif group
        NAMED_INACTIVE_THRESHOLD
      end

      config = {
        durable_name: group,
        filter_subject: subject,
        deliver_policy: Config.choice(settings, DELIVER_POLICY_KEY, %w[all new]),
        ack_policy: "none",
        inactive_threshold: Config.duration(settings, CONSUMER_INACTIVE_THRESHOLD_KEY, default_inactive)
      }
      acks = {
        ack_policy: "explicit",
        ack_wait: Config.duration(settings, ACK_WAIT_KEY, nil),
        max_deliver: Config.positive_integer(settings, MAX_DELIVER_KEY, max_deliver),
        backoff: Config.durations(settings, BACKOFF_KEY),
        max_ack_pending: Config.positive_integer(settings, MAX_ACK_PENDING_KEY, nil)
      }
      if create && ack_policy != :none && acks[:backoff] && acks[:backoff].length >= acks[:max_deliver]
        raise BadConfig, "#{MAX_DELIVER_KEY}=#{acks[:max_deliver]} must exceed the #{acks[:backoff].length} entries of #{BACKOFF_KEY}"
      end

      config = config.merge(acks) unless ack_policy == :none
      config = config.merge(mem_storage: true, num_replicas: 1) if ordered
      config.compact
    end
    private_class_method :consumer_config

    # Whether the subscriber uses an ordered consumer.
    def ordered?
      self.class.ordered?(group, ack_policy)
    end

    # Raises BadConfig unless the pull settings are within nats.go's limits.
    # The runtime calls it at start, where nats.go checks them.
    def check_pull_limits!
      raise BadConfig, "#{PULL_EXPIRY_KEY}=#{pull_expiry} must be at least 1 second" if pull_expiry < 1
      unless pull_heartbeat.between?(0.5, 30)
        raise BadConfig, "#{PULL_HEARTBEAT_KEY}=#{pull_heartbeat} must be from 0.5 to 30 seconds"
      end
      return if pull_heartbeat <= pull_expiry / 2

      raise BadConfig, "#{PULL_HEARTBEAT_KEY}=#{pull_heartbeat} must be no more than half of #{PULL_EXPIRY_KEY}=#{pull_expiry}"
    end
  end

  # Checks endpoint subjects against streams, and describes JetStream
  # deliveries.
  module JetStream
    # The JetStream API error codes for JetStream not enabled on the server,
    # and not enabled for the account.
    JETSTREAM_NOT_ENABLED = [10_076, 10_039].freeze

    # Raises StreamCoversRoute when a stream covers the subject of an endpoint
    # in +subscriptions+. A server without JetStream has no streams, so the
    # check passes when nothing answers the lookup or JetStream is not
    # enabled. Other nats-pure errors pass through unchanged.
    def self.check_endpoint_streams(jetstream, subscriptions)
      subscriptions.each do |subscription|
        next unless subscription.replies

        begin
          name = jetstream.find_stream_name_by_subject(subscription.subject)
        rescue NATS::JetStream::Error::NotFound
          next
        rescue NATS::JetStream::Error::ServiceUnavailable => error
          # nats-pure raises it without an error code when nothing answered.
          return nil if error.err_code.nil? || JETSTREAM_NOT_ENABLED.include?(error.err_code)

          raise
        end
        raise StreamCoversRoute.new(subscription.subject, name)
      end
      nil
    end

    # Adds +delivery+, a message's JetStream metadata from nats-pure, to the
    # handler's metadata.
    def self.delivery_metadata(metadata, delivery)
      metadata[ServiceMesh::DELIVERY_ATTEMPT_KEY] = delivery.num_delivered.to_s
      metadata[STREAM_METADATA_KEY] = delivery.stream
      metadata[CONSUMER_METADATA_KEY] = delivery.consumer
      metadata[STREAM_SEQUENCE_METADATA_KEY] = delivery.sequence.stream.to_s
      metadata[CONSUMER_SEQUENCE_METADATA_KEY] = delivery.sequence.consumer.to_s
      metadata[NUM_PENDING_METADATA_KEY] = delivery.num_pending.to_s
      metadata[TIMESTAMP_METADATA_KEY] = delivery.timestamp.utc.iso8601(9)
    end
  end
  private_constant :JetStream

  # A JetStream message on its way from a pull loop to its handler. It is
  # tracked from when the runtime receives it until its handler's result or
  # Runtime#stop settles it, whichever comes first.
  class Delivery
    attr_reader :msg, :subscription, :pull_loop, :arrival

    def initialize(msg, subscription, pull_loop, arrival)
      @msg = msg
      @subscription = subscription
      @pull_loop = pull_loop
      @arrival = arrival
      @state = :waiting
      @lock = Mutex.new
    end

    # Marks the handler running, and returns false when stop already
    # settled the delivery.
    def begin
      @lock.synchronize do
        next false if @state == :settled

        @state = :running
        true
      end
    end

    # Marks the delivery settled. Returns [running, ok]: ok is false when it
    # already was settled, and running tells whether its handler had begun.
    def settle
      @lock.synchronize do
        next [false, false] if @state == :settled

        running = @state == :running
        @state = :settled
        [running, true]
      end
    end
  end
  private_constant :Delivery
end
