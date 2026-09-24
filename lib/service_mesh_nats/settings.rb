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

  DEFAULT_URL = "nats://127.0.0.1:4222"
  DEFAULT_CONNECT_TIMEOUT = 5.0
  DEFAULT_REQUEST_TIMEOUT = 30.0

  # Value parsers shared by the two settings types.
  module Settings
    module_function

    # Reads +key+ as a positive number of seconds, or returns +fallback+ when
    # absent.
    def duration(hash, key, fallback)
      return fallback unless hash.key?(key)

      value = Float(hash[key], exception: false)
      raise BadConfig, "#{key}=#{hash[key].inspect} must be a positive number of seconds" if value.nil? || value <= 0

      value
    end

    def positive_integer(hash, key, fallback)
      return fallback unless hash.key?(key)

      value = Integer(hash[key], 10, exception: false)
      raise BadConfig, "#{key}=#{hash[key].inspect} must be a positive integer" if value.nil? || value <= 0

      value
    end
  end

  # The keys a Client reads. Any other key is ignored.
  ClientSettings = Data.define(:url, :name, :connect_timeout, :request_timeout) do
    def self.parse(config)
      config = config.to_h
      new(
        url: config.fetch(URL_KEY, DEFAULT_URL).then { |v| v.to_s.empty? ? DEFAULT_URL : v },
        name: config[NAME_KEY],
        connect_timeout: Settings.duration(config, CONNECT_TIMEOUT_KEY, DEFAULT_CONNECT_TIMEOUT),
        request_timeout: Settings.duration(config, REQUEST_TIMEOUT_KEY, DEFAULT_REQUEST_TIMEOUT)
      )
    end

    def connect_options
      opts = {servers: [url], connect_timeout: connect_timeout}
      opts[:name] = name if name && !name.empty?
      opts
    end
  end

  # The keys a Runtime reads. Connection keys in a runtime's config are
  # ignored; the runtime's client carries them.
  RuntimeSettings = Data.define(:deployment_group, :concurrency) do
    def self.parse(config)
      config = config.to_h
      deployment_group = config[ServiceMesh::DEPLOYMENT_GROUP_KEY].to_s
      raise ServiceMesh::NoDeploymentGroup if deployment_group.empty?

      new(
        deployment_group: deployment_group,
        concurrency: Settings.positive_integer(config, CONCURRENCY_KEY, Etc.nprocessors)
      )
    end
  end
end
