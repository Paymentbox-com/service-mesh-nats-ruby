# frozen_string_literal: true

require "json"
require "nats/io/client"

module ServiceMeshNats
  # Creates and looks up JetStream consumers with the server's API directly,
  # so durations travel in nanoseconds. nats-pure's own consumer calls take
  # ack_wait and inactive_threshold in whole seconds and read them back
  # rounded down, which would lose fractional settings and make
  # ServiceMesh::DEADLINE_KEY early.
  #
  # Errors are nats-pure's: NATS::JetStream::Error::ServiceUnavailable when
  # JetStream does not answer, and the NATS::JetStream::Error::APIError
  # subclass nats-pure raises for each error the server returns.
  module ConsumerAPI
    API_PREFIX = "$JS.API"
    API_TIMEOUT = 5.0
    NANOSECONDS = 1_000_000_000

    # Creates the consumer +name+ on +stream+ from +config+, a
    # JetStreamSettings#consumer_config, or updates it when +update+ is true
    # and it exists. Returns the Consumer the server reports.
    def self.create(connection, stream, name, config, update:)
      body = {stream_name: stream, config: wire_config(config.merge(name: name))}
      body[:action] = "create" unless update
      subject = "#{API_PREFIX}.CONSUMER.CREATE.#{stream}.#{name}.#{config.fetch(:filter_subject)}"
      consumer(request(connection, subject, body.to_json))
    end

    # Looks up the consumer +name+ on +stream+.
    def self.info(connection, stream, name)
      consumer(request(connection, "#{API_PREFIX}.CONSUMER.INFO.#{stream}.#{name}", ""))
    end

    # Deletes +consumer+, a Consumer opened for the subscriber with
    # JetStreamSettings +settings+. Raises ArgumentError unless the
    # subscriber uses an ordered consumer, which belongs to one instance, so
    # a consumer shared by a named group is never deleted.
    def self.delete(connection, consumer, settings)
      raise ArgumentError, "only an ordered consumer is deleted, not #{consumer.name}" unless settings.ordered?

      request(connection, "#{API_PREFIX}.CONSUMER.DELETE.#{consumer.stream}.#{consumer.name}", "")
      nil
    end

    # The config as the server takes it, with durations in nanoseconds.
    def self.wire_config(config)
      config.to_h do |key, value|
        case key
        when :ack_wait, :inactive_threshold then [key, nanoseconds(value)]
        when :backoff then [key, value.map { |seconds| nanoseconds(seconds) }]
        else [key, value]
        end
      end
    end

    def self.nanoseconds(seconds)
      (seconds * NANOSECONDS).round
    end

    def self.consumer(result)
      config = result.fetch(:config)
      Consumer.new(
        stream: result.fetch(:stream_name),
        name: result.fetch(:name),
        acks: config[:ack_policy] != "none",
        ack_wait: config[:ack_wait] && Rational(config[:ack_wait], NANOSECONDS),
        backoff: config[:backoff]&.map { |duration| Rational(duration, NANOSECONDS) },
        max_deliver: config.fetch(:max_deliver, -1)
      )
    end

    def self.request(connection, subject, body)
      reply = begin
        connection.request(subject, body, timeout: API_TIMEOUT)
      rescue NATS::IO::NoRespondersError
        raise NATS::JetStream::Error::ServiceUnavailable
      end
      result = JSON.parse(reply.data, symbolize_names: true)
      raise api_error(result[:error]) if result[:error]

      result
    end

    # The nats-pure error for an API error, chosen by the same status and
    # error codes nats-pure uses.
    def self.api_error(details)
      errors = NATS::JetStream::Error
      case details[:code]
      when 503 then errors::ServiceUnavailable.new(details)
      when 500 then errors::ServerError.new(details)
      when 404
        case details[:err_code]
        when 10059 then errors::StreamNotFound.new(details)
        when 10014 then errors::ConsumerNotFound.new(details)
        else errors::NotFound.new(details)
        end
      when 400
        case details[:err_code]
        when 10148 then errors::ConsumerAlreadyExists.new(details)
        when 10149 then errors::ConsumerDoesNotExist.new(details)
        else errors::BadRequest.new(details)
        end
      else errors::APIError.new(details)
      end
    end

    private_class_method :wire_config, :nanoseconds, :consumer, :request, :api_error
  end
  private_constant :ConsumerAPI
end
