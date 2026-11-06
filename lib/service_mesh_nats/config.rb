# frozen_string_literal: true

require "etc"

module ServiceMeshNats
  # Configuration keys this transport reads beyond ServiceMesh::DEPLOYMENT_GROUP_KEY.
  # Every value is a string. Durations are seconds, such as "5" or "0.25".
  URL_KEY = "url"
  NAME_KEY = "name"
  CONNECT_TIMEOUT_KEY = "connect_timeout"
  REQUEST_TIMEOUT_KEY = "request_timeout"
  CONCURRENCY_KEY = "concurrency"
  # The most times one JetStream message is delivered, as a positive integer.
  # In a runtime's config it is the default for subscribers that do not set
  # it. It is also a subscriber setting.
  MAX_DELIVER_KEY = "max_deliver"

  # JetStream settings, read from an Endpoint's or Subscriber's metadata over
  # its Target's metadata, and from a call's options over its Target's
  # metadata. Every key but STREAM_KEY is read only from a Subscriber that
  # sets STREAM_KEY.
  #
  # The stream name. A Subscriber consumes from a consumer on it, and a
  # publish waits for its ack. On an Endpoint or a request it is BadConfig.
  STREAM_KEY = "stream"
  # "true" or "false". "false" leaves a named consumer group's consumer to be
  # created outside the runtime.
  CREATE_CONSUMER_KEY = "create_consumer"
  # "double", "explicit", or "none".
  ACK_POLICY_KEY = "ack_policy"
  # Seconds the server waits for an ack before it sends a message again.
  # Ignored when BACKOFF_KEY is set.
  ACK_WAIT_KEY = "ack_wait"
  # Comma-separated seconds, one for each delivery in turn, that set how long
  # the server waits for an ack before it sends the message again. Deliveries
  # past the end of the list use its last entry. The server sets the
  # consumer's ack wait to the first entry. MAX_DELIVER_KEY must exceed its
  # length.
  BACKOFF_KEY = "backoff"
  # Seconds the server waits before it sends a failed message again.
  NAK_DELAY_KEY = "nak_delay"
  # The most unacked messages the consumer allows at once.
  MAX_ACK_PENDING_KEY = "max_ack_pending"
  # "all" or "new", where a newly created consumer starts.
  DELIVER_POLICY_KEY = "deliver_policy"
  # Seconds a consumer outlives its last puller.
  CONSUMER_INACTIVE_THRESHOLD_KEY = "consumer_inactive_threshold"
  # Seconds the server keeps a pull open while it has nothing to send. At
  # least 1. Also a runtime config key, where it is the default for
  # subscribers that do not set it.
  PULL_EXPIRY_KEY = "pull_expiry"
  # Seconds between the server's heartbeats on an open pull that has nothing
  # to send. From 0.5 to 30, and no more than half of PULL_EXPIRY_KEY. Two
  # missed heartbeats make the subscriber pull again. Also a
  # runtime config key, where it is the default for subscribers that do not
  # set it.
  PULL_HEARTBEAT_KEY = "pull_heartbeat"

  DEFAULT_URL = "nats://127.0.0.1:4222"
  DEFAULT_CONNECT_TIMEOUT = 5.0
  DEFAULT_REQUEST_TIMEOUT = 30.0
  DEFAULT_MAX_DELIVER = 5
  NAMED_INACTIVE_THRESHOLD = 7 * 24 * 60 * 60.0
  ORDERED_INACTIVE_THRESHOLD = 5 * 60.0
  DEFAULT_PULL_EXPIRY = 30.0

  # Value parsers shared by the settings types.
  module Config
    # Reads +key+ as a positive number of seconds, or returns +fallback+ when
    # absent.
    def self.duration(hash, key, fallback)
      return fallback unless hash.key?(key)

      value = Float(hash[key], exception: false)
      raise BadConfig, "#{key}=#{hash[key].inspect} must be a positive number of seconds" if value.nil? || value <= 0

      value
    end

    def self.positive_integer(hash, key, fallback)
      return fallback unless hash.key?(key)

      value = Integer(hash[key], 10, exception: false)
      raise BadConfig, "#{key}=#{hash[key].inspect} must be a positive integer" if value.nil? || value <= 0

      value
    end

    # Reads +key+ as one of the strings in +allowed+, or returns the first of
    # them when absent.
    def self.choice(hash, key, allowed)
      return allowed.first unless hash.key?(key)
      return hash[key] if allowed.include?(hash[key])

      raise BadConfig, "#{key}=#{hash[key].inspect} must be one of #{allowed.map(&:inspect).join(", ")}"
    end

    # Reads +key+ as a comma-separated list of positive numbers of seconds,
    # or returns nil when absent.
    def self.durations(hash, key)
      return nil unless hash.key?(key)

      hash[key].to_s.split(",", -1).map do |part|
        value = Float(part.strip, exception: false)
        if value.nil? || value <= 0
          raise BadConfig, "#{key}=#{hash[key].inspect} must be a comma-separated list of positive numbers of seconds, such as \"1,5\""
        end

        value
      end
    end
  end

  # The keys a Client reads. Any other key is ignored.
  ClientSettings = Data.define(:url, :name, :connect_timeout, :request_timeout) do
    def self.parse(config)
      config = config.to_h
      new(
        url: config.fetch(URL_KEY, DEFAULT_URL).then { |url| url.to_s.empty? ? DEFAULT_URL : url },
        name: config[NAME_KEY],
        connect_timeout: Config.duration(config, CONNECT_TIMEOUT_KEY, DEFAULT_CONNECT_TIMEOUT),
        request_timeout: Config.duration(config, REQUEST_TIMEOUT_KEY, DEFAULT_REQUEST_TIMEOUT)
      )
    end

    def connect_options
      options = {servers: [url], connect_timeout: connect_timeout}
      options[:name] = name if name && !name.empty?
      options
    end
  end

  # The keys a Runtime reads. Connection keys in a runtime's config are
  # ignored, because the runtime's client carries them.
  #
  # +pull_expiry+ and +pull_heartbeat+ are nil when the config does not set
  # them.
  RuntimeSettings = Data.define(:deployment_group, :concurrency, :max_deliver, :pull_expiry, :pull_heartbeat) do
    def self.parse(config)
      config = config.to_h
      deployment_group = config[ServiceMesh::DEPLOYMENT_GROUP_KEY].to_s
      raise ServiceMesh::NoDeploymentGroup if deployment_group.empty?

      new(
        deployment_group: deployment_group,
        concurrency: Config.positive_integer(config, CONCURRENCY_KEY, Etc.nprocessors),
        max_deliver: Config.positive_integer(config, MAX_DELIVER_KEY, DEFAULT_MAX_DELIVER),
        pull_expiry: Config.duration(config, PULL_EXPIRY_KEY, nil),
        pull_heartbeat: Config.duration(config, PULL_HEARTBEAT_KEY, nil)
      )
    end
  end
end
