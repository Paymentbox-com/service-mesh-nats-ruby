# frozen_string_literal: true

require "time"

module ServiceMeshNats
  class Runtime
    private

    # Handles a message a pull loop took a permit for. +pool+ is the pool of
    # the start that opened the pull loop.
    def receive(pool, subscription, pull_loop, msg, arrival)
      delivery = Delivery.new(msg, subscription, pull_loop, arrival)
      acks = acks?(subscription, pull_loop.consumer)
      # The delivery is tracked before dispatch so stop can nak it even while
      # it waits for the pool. Otherwise the server holds it until its ack
      # wait runs out.
      @pending_lock.synchronize { @pending[delivery] = true } if acks
      dispatched = dispatch(pool, permit_held: true) { serve_jetstream(delivery) }
      abandon(delivery) if acks && !dispatched
    end

    # Runs a JetStream delivery's handler on a pool thread, and settles the
    # delivery by its result unless stop already settled it.
    def serve_jetstream(delivery)
      subscription = delivery.subscription
      consumer = delivery.pull_loop.consumer
      acks = acks?(subscription, consumer)
      return if acks && !delivery.begin

      msg = delivery.msg
      metadata = Header.incoming_metadata(msg.header)
      meta = begin
        msg.metadata
      rescue NATS::JetStream::Error::NotJSMessage => error
        @logger.warn(Log.line("message has no JetStream metadata", subject: subscription.subject, error: error.message))
        nil
      end
      if meta
        JetStream.delivery_metadata(metadata, meta)
        if acks
          deadline = consumer.deadline(delivery.arrival, meta.num_delivered)
          metadata[ServiceMesh::DEADLINE_KEY] = deadline.utc.iso8601(9)
          now = Time.now
          log_late_start(delivery, meta, metadata, now - deadline) if now > deadline
        end
      end

      failure = nil
      begin
        inbound = ServiceMesh::Message.new(target: subscription.target, metadata: metadata, payload: msg.data.to_s)
        subscription.handler.call(inbound)
      rescue => error
        failure = error
        @logger.error(Log.line("subscriber handler failed", subject: subscription.subject,
          stream: subscription.jetstream.stream, delivery_attempt: meta&.num_delivered,
          error: "#{error.class}: #{error.message}"))
      end
      # Without acks there is nothing to settle, and stop never tracks these
      # deliveries.
      return unless acks

      _running, ok = delivery.settle
      return unless ok

      untrack(delivery)
      if failure
        begin
          nak(delivery)
        rescue => error
          @logger.error(Log.line("nak failed", subject: subscription.subject, stream: subscription.jetstream.stream,
            error: "#{error.class}: #{error.message}"))
        end
        log_final_failure(delivery, meta, metadata) if meta && consumer.last_delivery?(meta.num_delivered)
        return
      end

      begin
        if subscription.jetstream.ack_policy == :double
          msg.ack_sync(timeout: @client.request_timeout)
        else
          msg.ack
        end
      rescue => error
        @logger.error(Log.line("ack failed", subject: subscription.subject, stream: subscription.jetstream.stream,
          error: "#{error.class}: #{error.message}"))
      end
    end

    # Logs a handler that started after its delivery's deadline, at most once
    # a minute for each subscriber, with how many late starts were not logged
    # since the last one that was.
    def log_late_start(delivery, meta, metadata, late_by)
      skipped = delivery.pull_loop.late_start_logged
      return unless skipped

      @logger.warn(Log.line("handler started after its ack deadline", **delivery_fields(delivery, meta, metadata),
        late_by: late_by.round(3), skipped: skipped))
    end

    # Logs a message whose handler failed on its last delivery.
    def log_final_failure(delivery, meta, metadata)
      @logger.error(Log.line("message will not be delivered again", **delivery_fields(delivery, meta, metadata),
        max_deliver: delivery.pull_loop.consumer.max_deliver))
    end

    # The fields that identify a JetStream delivery in a log line.
    def delivery_fields(delivery, meta, metadata)
      {
        subject: delivery.subscription.subject,
        stream: meta.stream,
        consumer: meta.consumer,
        stream_sequence: meta.sequence.stream,
        delivery_attempt: meta.num_delivered,
        message_id: metadata[ServiceMesh::MESSAGE_ID_KEY]
      }
    end

    def nak(delivery)
      delay = delivery.subscription.jetstream.nak_delay
      delay ? delivery.msg.nak(delay: (delay * PullLoop::NANOSECONDS).round) : delivery.msg.nak
    end

    def untrack(delivery)
      @pending_lock.synchronize { @pending.delete(delivery) }
    end

    # Closes intake, naks every delivery not yet settled, and has each pull
    # loop give up on the messages it still holds, which are nacked too.
    def abandon_drain(pull_loops)
      self.accepting = false
      abandon_pending
      pull_loops.each do |subscription, pull_loop|
        pull_loop.abandon do |msg|
          next unless acks?(subscription, pull_loop.consumer)

          abandon(Delivery.new(msg, subscription, pull_loop, Time.now))
        end
      end
    end

    # Naks every delivery not yet settled.
    def abandon_pending
      deliveries = @pending_lock.synchronize { @pending.keys }
      deliveries.each { |delivery| abandon(delivery) }
    end

    # Naks a delivery stop gives up on and logs it, unless it is already
    # settled.
    def abandon(delivery)
      running, ok = delivery.settle
      return unless ok

      untrack(delivery)
      subscription = delivery.subscription
      msg = delivery.msg
      meta = begin
        msg.metadata
      rescue NATS::JetStream::Error::NotJSMessage
        nil
      end
      error = begin
        nak(delivery)
        nil
      rescue => nak_error
        "#{nak_error.class}: #{nak_error.message}"
      end
      @logger.warn(Log.line("nacked a message at the end of the drain", subject: subscription.subject,
        stream: subscription.jetstream.stream, consumer: meta&.consumer, stream_sequence: meta&.sequence&.stream,
        message_id: Header.incoming_metadata(msg.header)[ServiceMesh::MESSAGE_ID_KEY], handler_running: running,
        error: error))
    end
  end
end
