# frozen_string_literal: true

require "json"
require "nats/io/client"

module ServiceMeshNats
  # Pulls one JetStream subscriber's messages from its consumer and hands them
  # to the runtime, one handler permit at a time.
  #
  # The loop keeps up to +concurrency+ messages owed by the server or waiting
  # for a permit. It sends a pull for the difference when that count drops
  # below half of +concurrency+, rounded up, or when a pull ends. Each pull
  # carries the subscriber's expiry and heartbeat. When neither a message nor
  # a heartbeat arrives for twice the heartbeat, the loop logs a warning and
  # pulls again, as nats.go does. After a reconnect it pulls again without a
  # warning. While the connection is down, it neither pulls nor checks
  # heartbeats.
  #
  # The loop does not create a missing consumer again. When the server
  # reports the consumer deleted, the loop logs an error and stops pulling,
  # and logs the error again every 5 seconds until it stops. When a pull gets
  # no responders, which is what a pull to a consumer deleted while the
  # connection was down gets, the loop logs an error and keeps pulling once a
  # second. Every 5 seconds while connected, it looks the consumer up, logs
  # the error again while it is missing, and stops logging once it exists.
  #
  # An ordered consumer is replaced instead. The loop checks that each
  # delivery's consumer sequence is one more than the last. The stream
  # sequence is not checked, because the consumer's filter and deleted
  # messages leave holes in it. On a skipped consumer sequence, missed
  # heartbeats, a deleted consumer, no responders, or a reconnect, the loop
  # deletes the consumer and creates a new one at the stream sequence after
  # the last it handed over, pulled on a new inbox. Handlers still run
  # concurrently and may finish out of order.
  #
  # Runtime#stop drains the loop. It stops pulling and keeps handing over the
  # messages already received, each once it gets a permit. When the drain
  # ends first, the loop gives up waiting and gives each message it has not
  # handed over to the runtime to nak.
  class PullLoop
    API_PREFIX = "$JS.API"
    NANOSECONDS = 1_000_000_000
    STATUS = "Status"
    DESCRIPTION = "Description"
    PENDING_MESSAGES = "Nats-Pending-Messages"
    CONSUMER_DELETED = "Consumer Deleted"
    LEADERSHIP_CHANGE = "Leadership Change"
    RETRY_DELAY = 1.0
    MISSING_LOG_INTERVAL = 5.0
    LATE_START_LOG_INTERVAL = 60.0
    PERMIT_POLL = 0.1
    DRAIN_QUIET = 0.1

    # The consumer the loop pulls from, which replacing an ordered consumer
    # changes.
    attr_reader :consumer

    # +consumer+ is the Consumer that start opened, and +settings+
    # the subscriber's JetStreamSettings. +permits+ is the runtime's
    # semaphore. +deliver+ is called with each message and the time it landed
    # on the inbox, after the loop took a permit for it, and the runtime
    # returns the permit when the handler finishes.
    def initialize(connection:, subject:, settings:, consumer:, concurrency:, permits:, logger:, &deliver)
      @connection = connection
      @subject = subject
      @settings = settings
      @consumer = consumer
      @concurrency = concurrency
      @threshold = (concurrency + 1) / 2
      @permits = permits
      @logger = logger
      @deliver = deliver

      @queue = Queue.new
      @count = 0
      @request_ended = true
      # nil, :stopped after the server reported the consumer deleted, or
      # :pulling after a pull got no responders.
      @missing = nil
      @next_missing_check = nil
      @consumer_sequence = 0
      @last_stream_sequence = nil
      @late_lock = Mutex.new
      @last_late_log = nil
      @late_skipped = 0
      @draining = false
      @abandon_nak = nil
      @unsubscribed = false
      @stopping = false
      @stopped = Queue.new
    end

    # Subscribes the inbox and starts the loop thread.
    def start
      subscribe_inbox
      @thread = Thread.new { run }
      self
    end

    # Makes the loop pull again, and replace an ordered consumer. The runtime
    # calls it when nats-pure reports a reconnect.
    def reset
      @queue << :reset
    end

    # Returns how many late starts were not logged since the last one that
    # was, when a late start now is logged, or nil when it is not. Handler
    # threads call it, so it takes a lock.
    def late_start_logged
      @late_lock.synchronize do
        now = monotonic
        if @last_late_log && now - @last_late_log < LATE_START_LOG_INTERVAL
          @late_skipped += 1
          next nil
        end

        skipped = @late_skipped
        @last_late_log = now
        @late_skipped = 0
        skipped
      end
    end

    # Unsubscribes the inbox and stops pulling. The loop keeps handing over
    # the messages already received, and ends once none are left.
    def drain
      @draining = true
      unsubscribe
      @queue << :drain
      @stopped << true
    end

    # Whether the loop has ended.
    def drained?
      !@thread&.alive?
    end

    # Ends a drain that has not finished. Each message the loop received and
    # did not hand over, including one waiting for a permit, is passed to
    # +nak+ instead. Returns once the loop has ended.
    def abandon(&nak)
      @abandon_nak = nak
      @queue << :abandon
      @thread&.join
      abandon_queued
    end

    # Stops pulling and handing out messages, and unsubscribes the inbox.
    def stop
      @stopping = true
      @stopped << true
      @queue << :stop
      @thread&.join
      unsubscribe
    end

    private

    # Subscribes a new inbox for pulls. Each message is queued with the inbox
    # it came on, so the loop can drop what an inbox it left behind still
    # receives.
    def subscribe_inbox
      inbox = @connection.new_inbox
      @inbox = inbox
      @unsubscribed = false
      @inbox_subscription = @connection.subscribe(inbox) { |msg| @queue << [inbox, msg, Time.now] }
    end

    # Unsubscribes the current inbox once. nats-pure raises
    # ConnectionClosedError when the client was closed directly. The server
    # dropped the subscription with the connection, so there is nothing left
    # to unsubscribe.
    def unsubscribe
      return if @unsubscribed

      @unsubscribed = true
      @inbox_subscription&.unsubscribe
    rescue NATS::IO::ConnectionClosedError
      nil
    end

    def run
      until @stopping
        break hand_over_received if @draining

        if @missing == :stopped
          log_missing
          wait(MISSING_LOG_INTERVAL)
          next
        end

        pull if (@count < @threshold || @request_ended) && @connection.connected?
        event = @queue.pop(timeout: 2 * @settings.pull_heartbeat)
        case event
        when :stop then break
        when :drain then next
        when :reset then reconnected
        when nil then missed_heartbeats
        else receive(*event)
        end
        check_missing if @missing == :pulling && monotonic >= @next_missing_check
      end
    rescue => error
      @logger.error(Log.line("pull loop stopped", subject: @subject, stream: @consumer.stream,
        error: "#{error.class}: #{error.message}"))
    end

    # Hands over what the inbox received before the drain, each message once
    # it gets a permit, until the queue stays empty and nats-pure holds
    # nothing more for the inbox. Once the drain is abandoned, each message
    # goes to the runtime to nak.
    def hand_over_received
      loop do
        event = @queue.pop(timeout: DRAIN_QUIET)
        break if event.nil? && @inbox_subscription.pending_queue.empty?
        next unless event.is_a?(Array)

        inbox, msg, arrival = event
        next unless inbox == @inbox && msg.header&.[](STATUS).nil?
        next if @settings.ordered? && !in_order?(msg)

        if take_permit
          @deliver.call(msg, arrival)
        else
          @abandon_nak.call(msg)
        end
      end
    end

    # Passes each message still queued after the loop ended to the runtime to
    # nak.
    def abandon_queued
      until @queue.empty?
        event = @queue.pop
        next unless event.is_a?(Array)

        _inbox, msg, _arrival = event
        @abandon_nak.call(msg) if msg.header&.[](STATUS).nil?
      end
    end

    # Asks for enough messages to bring the count back to +concurrency+.
    def pull
      @request_ended = false
      batch = @concurrency - @count
      return unless batch.positive?

      body = {
        batch: batch,
        expires: (@settings.pull_expiry * NANOSECONDS).round,
        idle_heartbeat: (@settings.pull_heartbeat * NANOSECONDS).round
      }
      @connection.publish("#{API_PREFIX}.CONSUMER.MSG.NEXT.#{@consumer.stream}.#{@consumer.name}", body.to_json, @inbox)
      @count += batch
    end

    def receive(inbox, msg, arrival)
      return unless inbox == @inbox

      status = msg.header&.[](STATUS)
      return handle_status(msg, status) if status
      return if @settings.ordered? && !in_order?(msg)
      unless take_permit
        # A drain abandoned while the message waited for a permit.
        @abandon_nak&.call(msg)
        return
      end

      # A message from a pull forgotten by forget_pulls is not counted.
      @count -= 1 if @count.positive?
      @deliver.call(msg, arrival)
    end

    def handle_status(msg, status)
      case status
      when "100"
        nil
      when "404", "408"
        ended(msg)
      when "503"
        no_responders
      when "409"
        case msg.header[DESCRIPTION]
        when CONSUMER_DELETED then deleted
        when LEADERSHIP_CHANGE
          @count = 0
          @request_ended = true
        else ended(msg)
        end
      else
        @logger.warn(Log.line("pull got an unexpected status", subject: @subject, stream: @consumer.stream,
          status: status, description: msg.header[DESCRIPTION]))
        ended(msg)
      end
    end

    # A pull ended early. The server reports how many of its messages it did
    # not send.
    def ended(msg)
      @count -= msg.header[PENDING_MESSAGES].to_i
      @count = 0 if @count.negative?
      @request_ended = true
    end

    # The server reported the consumer deleted.
    def deleted
      return replace_consumer("the server reported the consumer deleted") if @settings.ordered?

      @missing = :stopped
    end

    # No consumer answered a pull, because it is gone or JetStream is
    # unavailable. The next pull waits a second, so the loop does not spin.
    def no_responders
      return replace_consumer("no consumer answered the pull") if @settings.ordered?

      unless @missing
        @missing = :pulling
        @next_missing_check = monotonic + MISSING_LOG_INTERVAL
        log_missing
      end
      forget_pulls
      wait(RETRY_DELAY)
    end

    # Logs the missing consumer again while a lookup does not find it, and
    # stops logging once it exists. Nothing is checked while the connection
    # is down.
    def check_missing
      @next_missing_check = monotonic + MISSING_LOG_INTERVAL
      return unless @connection.connected?

      ConsumerAPI.info(@connection, @consumer.stream, @consumer.name)
      @missing = nil
    rescue NATS::JetStream::Error::ConsumerNotFound
      log_missing
    rescue NATS::IO::Error => error
      log_missing(error)
    end

    def log_missing(error = nil)
      @logger.error(Log.line("consumer missing", subject: @subject, stream: @consumer.stream, consumer: @consumer.name,
        error: error && "#{error.class}: #{error.message}"))
    end

    # Records an ordered consumer's sequences and returns true when +msg+ is
    # its next delivery. Otherwise it replaces the consumer and returns false,
    # and the new consumer sends the message again.
    def in_order?(msg)
      sequence = msg.metadata.sequence
      unless sequence.consumer == @consumer_sequence + 1
        # A draining loop pulls nothing more, so it does not replace the
        # consumer.
        return false if @draining

        replace_consumer("expected consumer sequence #{@consumer_sequence + 1}, got #{sequence.consumer}")
        return false
      end

      @consumer_sequence = sequence.consumer
      @last_stream_sequence = sequence.stream
      true
    end

    # Waits for a handler permit, or returns false when the loop is stopping
    # or its drain was abandoned.
    def take_permit
      until @permits.try_acquire(1, PERMIT_POLL)
        return false if @stopping || @abandon_nak
      end
      true
    end

    # Neither a message nor a heartbeat arrived for twice the heartbeat.
    # While the connection is down, nothing is pulled or checked, because
    # nats-pure would send every pull at once on reconnect, and the reconnect
    # makes the loop pull again anyway.
    def missed_heartbeats
      return unless @connection.connected?

      reason = "no message or heartbeat for #{2 * @settings.pull_heartbeat} seconds"
      return replace_consumer(reason) if @settings.ordered?

      @logger.warn(Log.line("consume error", subject: @subject, stream: @consumer.stream, error: reason))
      forget_pulls
    end

    # nats-pure reported a reconnect. Pulls sent before it are lost, so the
    # loop pulls again, and an ordered consumer is replaced.
    def reconnected
      return replace_consumer("nats-pure reported a reconnect") if @settings.ordered?

      forget_pulls
    end

    def forget_pulls
      @count = 0
      @request_ended = true
    end

    # Replaces an ordered consumer with a new one on a new inbox, so nothing
    # still sent for the old consumer's pulls is taken as the new one's.
    # Creates that fail are tried again after a delay, until the loop stops.
    def replace_consumer(reason)
      @logger.warn(Log.line("replacing the ordered consumer", subject: @subject, stream: @consumer.stream,
        consumer: @consumer.name, reason: reason))
      forget_pulls
      unsubscribe
      subscribe_inbox
      delete_consumer
      until @stopping || @draining
        return if create_replacement

        wait(RETRY_DELAY)
      end
    end

    # Deletes the ordered consumer. A failure is ignored, as nats.go ignores
    # it, because the consumer ends on its own after its inactive threshold.
    def delete_consumer
      ConsumerAPI.delete(@connection, @consumer, @settings)
    rescue NATS::JetStream::Error::ConsumerNotFound
      # Already gone, as when it was deleted from outside.
    rescue NATS::IO::Error => error
      @logger.warn(Log.line("deleting the ordered consumer failed", subject: @subject, stream: @consumer.stream,
        consumer: @consumer.name, error: "#{error.class}: #{error.message}"))
    end

    # Creates the ordered consumer's replacement at the stream sequence after
    # the last handed over, or with the subscriber's deliver_policy when
    # nothing was. Returns false when the create failed.
    def create_replacement
      config = @settings.consumer_config
      if @last_stream_sequence
        config = config.merge(deliver_policy: "by_start_sequence", opt_start_seq: @last_stream_sequence + 1)
      end
      @consumer = ConsumerAPI.create(@connection, @consumer.stream, NATS::NUID.next, config, update: false)
      @consumer_sequence = 0
      true
    rescue NATS::IO::Error => error
      @logger.warn(Log.line("creating the ordered consumer failed", subject: @subject, stream: @consumer.stream,
        error: "#{error.class}: #{error.message}"))
      false
    end

    def wait(seconds)
      @stopped.pop(timeout: seconds)&.then { @stopped << true }
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
  private_constant :PullLoop
end
