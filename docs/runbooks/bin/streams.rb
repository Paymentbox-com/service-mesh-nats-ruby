# frozen_string_literal: true

# Creates, inspects, and deletes the JetStream streams and consumers the
# local behavior runbook uses, so the runbook needs no tool beyond Ruby and
# nats-server.
#
#   bundle exec ruby docs/runbooks/bin/streams.rb add DEMO_CREATED demo.created
#   bundle exec ruby docs/runbooks/bin/streams.rb info DEMO_CREATED
#   bundle exec ruby docs/runbooks/bin/streams.rb consumers DEMO_CREATED
#   bundle exec ruby docs/runbooks/bin/streams.rb add-consumer DEMO_CREATED audit demo.created
#   bundle exec ruby docs/runbooks/bin/streams.rb rm-consumer DEMO_CREATED audit
#   bundle exec ruby docs/runbooks/bin/streams.rb rm DEMO_CREATED

require "json"
require "nats/io/client"

$stdout.sync = true

USAGE = <<~TEXT
  usage:
    streams.rb add NAME SUBJECT [limits|interest|workqueue]
    streams.rb info NAME
    streams.rb consumers NAME
    streams.rb add-consumer STREAM NAME SUBJECT
    streams.rb rm-consumer STREAM NAME
    streams.rb rm NAME
TEXT

command, *args = ARGV
abort USAGE if command.nil? || args.empty?

connection = NATS.connect(ENV.fetch("NATS_URL", "nats://127.0.0.1:4222"))
jetstream = connection.jetstream
seconds = ->(nanoseconds) { "#{nanoseconds.to_i / 1e9}s" }

begin
  case command
  when "add"
    name, subject, retention = args
    info = jetstream.add_stream(name: name, subjects: [subject], retention: retention || "limits")
    puts "created stream #{info.config.name.inspect} for #{info.config.subjects} with #{info.config.retention} retention"
  when "info"
    info = jetstream.stream_info(args[0])
    state = info.state
    puts "stream #{info.config.name.inspect}: subjects #{info.config.subjects}, #{state.messages} messages, " \
      "sequences #{state.first_seq} to #{state.last_seq}, #{state.consumer_count} consumers"
  when "consumers"
    # The raw API replies, so durations show in full rather than in
    # nats-pure's whole seconds.
    names = JSON.parse(connection.request("$JS.API.CONSUMER.NAMES.#{args[0]}", "", timeout: 5).data)["consumers"] || []
    names.each do |name|
      info = JSON.parse(connection.request("$JS.API.CONSUMER.INFO.#{args[0]}.#{name}", "", timeout: 5).data)
      config = info.fetch("config")
      puts "consumer #{name.inspect}: pending #{info["num_pending"]}, ack pending #{info["num_ack_pending"]}, " \
        "redelivered #{info["num_redelivered"]}, ack_policy=#{config["ack_policy"]} " \
        "ack_wait=#{seconds.call(config["ack_wait"])} max_deliver=#{config["max_deliver"]} " \
        "inactive_threshold=#{seconds.call(config["inactive_threshold"])}"
    end
  when "add-consumer"
    stream, name, subject = args
    jetstream.add_consumer(stream, durable_name: name, filter_subject: subject, ack_policy: "explicit", ack_wait: 60)
    puts "created consumer #{name.inspect} on stream #{stream.inspect} with ack_wait=60s"
  when "rm-consumer"
    jetstream.delete_consumer(args[0], args[1])
    puts "deleted consumer #{args[1].inspect} on stream #{args[0].inspect}"
  when "rm"
    jetstream.delete_stream(args[0])
    puts "deleted stream #{args[0].inspect}"
  else
    abort USAGE
  end
rescue NATS::IO::Error => error
  abort "#{command}: #{error.class}: #{error.message}"
ensure
  connection.close
end
