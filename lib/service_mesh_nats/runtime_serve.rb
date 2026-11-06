# frozen_string_literal: true

require "nats/io/client"
require "time"

module ServiceMeshNats
  class Runtime
    private

    # Handles a message from a core subscription on a pool thread. +arrival+
    # is when the message reached the subscription's callback.
    def serve(subscription, msg, arrival)
      metadata = Header.incoming_metadata(msg.header)
      add_deadline(metadata, arrival) if subscription.replies
      inbound = ServiceMesh::Message.new(target: subscription.target, metadata: metadata, payload: msg.data.to_s)

      unless subscription.replies
        begin
          subscription.handler.call(inbound)
        rescue => error
          @logger.error(Log.line("subscriber handler failed", subject: subscription.subject,
            error: "#{error.class}: #{error.message}"))
        end
        return
      end

      begin
        result = subscription.handler.call(inbound)
        unless result.is_a?(ServiceMesh::Message)
          raise TypeError, "endpoint #{subscription.subject} handler returned #{result.class}, expected ServiceMesh::Message"
        end
      rescue => error
        failure = error
        @logger.error(Log.line("endpoint handler failed", subject: subscription.subject,
          error: "#{error.class}: #{error.message}"))
      end
      return unless msg.reply

      if failure
        reply(subscription, msg.reply, {ServiceMesh::HANDLER_ERROR_KEY => failure.message}, "")
      else
        reply(subscription, msg.reply, result.metadata, result.payload)
      end
    end

    # Sets ServiceMesh::DEADLINE_KEY on a request that carries
    # ServiceMesh::TIMEOUT_KEY, to its arrival time plus the timeout. A timeout
    # that is not a whole number of milliseconds is ignored.
    def add_deadline(metadata, arrival)
      milliseconds = Integer(metadata[ServiceMesh::TIMEOUT_KEY].to_s, 10, exception: false)
      return if milliseconds.nil? || milliseconds.negative?

      metadata[ServiceMesh::DEADLINE_KEY] = (arrival + Rational(milliseconds, 1000)).utc.iso8601(9)
    end

    def reply(subscription, subject, metadata, payload)
      @connection.publish_msg(NATS::Msg.new(subject: subject, data: payload.to_s.b, header: Header.to_header(metadata)))
    rescue => error
      @logger.error(Log.line("reply failed", subject: subscription.subject, error: "#{error.class}: #{error.message}"))
    end
  end
end
