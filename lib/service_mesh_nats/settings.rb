# frozen_string_literal: true

require "etc"

module ServiceMeshNats
  # Configuration keys this runtime reads beyond DEPLOYMENT_GROUP_KEY. Every
  # value is a string. Durations are seconds, such as "5" or "0.25".
  URL_KEY = "url"
  NAME_KEY = "name"
  CONNECT_TIMEOUT_KEY = "connect_timeout"
  REQUEST_TIMEOUT_KEY = "request_timeout"
  CONCURRENCY_KEY = "concurrency"

  DEFAULT_URL = "nats://127.0.0.1:4222"
  DEFAULT_CONNECT_TIMEOUT = 5.0
  DEFAULT_REQUEST_TIMEOUT = 30.0

  # A parsed configuration hash.
  Settings = Data.define(:url, :name, :deployment_group, :connect_timeout, :request_timeout, :concurrency) do
    # +require_deployment_group+ is true for a Runtime and false for a
    # standalone Client, which ignores the key.
    def self.parse(config, require_deployment_group:)
      config = config.to_h
      deployment_group = config[DEPLOYMENT_GROUP_KEY].to_s
      raise NoDeploymentGroup if require_deployment_group && deployment_group.empty?

      new(
        url: config.fetch(URL_KEY, DEFAULT_URL).then { |v| v.to_s.empty? ? DEFAULT_URL : v },
        name: config[NAME_KEY],
        deployment_group: deployment_group,
        connect_timeout: duration(config, CONNECT_TIMEOUT_KEY, DEFAULT_CONNECT_TIMEOUT),
        request_timeout: duration(config, REQUEST_TIMEOUT_KEY, DEFAULT_REQUEST_TIMEOUT),
        concurrency: positive_integer(config, CONCURRENCY_KEY, Etc.nprocessors)
      )
    end

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

    def connect_options
      opts = {servers: [url], connect_timeout: connect_timeout}
      opts[:name] = name if name && !name.empty?
      opts
    end
  end
end
