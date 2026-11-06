# frozen_string_literal: true

# Sends requests or publishes and prints each result, naming the error when
# there is one.
#
#   bundle exec ruby docs/runbooks/bin/send.rb --kind route --subject demo.echo --n 3

require "optparse"
require_relative "../../../lib/service_mesh_nats"

$stdout.sync = true

options = {kind: "topic", subject: "demo.created", n: 1, payload: "hello", stream_info: false,
           cfg: {}, target: {}, set: {}, md: {}}
pair = ->(text) { text.split("=", 2).then { |key, value| [key, value.to_s] } }
OptionParser.new do |parser|
  parser.on("--kind KIND", %w[route topic], "route to request, topic to publish") { options[:kind] = _1 }
  parser.on("--subject SUBJECT", "the target's subject") { options[:subject] = _1 }
  parser.on("--n N", Integer, "how many messages to send") { options[:n] = _1 }
  parser.on("--payload PAYLOAD", "the payload, to which each message appends its number") { options[:payload] = _1 }
  parser.on("--stream-info", "after sending, print the target's stream from client.stream_info") { options[:stream_info] = true }
  parser.on("--cfg KEY=VALUE", "a key of the config given to Client.new") { options[:cfg].store(*pair.call(_1)) }
  parser.on("--target KEY=VALUE", "a key of the target's metadata") { options[:target].store(*pair.call(_1)) }
  parser.on("--set KEY=VALUE", "a per-call option") { options[:set].store(*pair.call(_1)) }
  parser.on("--md KEY=VALUE", "a key of the message's metadata") { options[:md].store(*pair.call(_1)) }
end.parse!

config = {"url" => ENV.fetch("NATS_URL", "nats://127.0.0.1:4222")}.merge(options[:cfg])
target = ServiceMesh::Target.new(segments: options[:subject].split("."), kind: options[:kind].to_sym, metadata: options[:target])
begin
  client = ServiceMeshNats::Client.new(config, ServiceMesh::ServiceMap.new(targets: [target]))
rescue => error
  puts "Client.new: #{error.class}: #{error.message}"
  exit 1
end

describe = lambda do |error|
  case error
  when ServiceMeshNats::HandlerError then "HandlerError #{error.text.inspect}"
  when ServiceMeshNats::BadConfig then "BadConfig: #{error.message}"
  else "#{error.class}: #{error.message}"
  end
end
milliseconds = ->(started) { "#{((Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1000).round}ms" }

1.upto(options[:n]) do |number|
  message = ServiceMesh::Message.new(target: target, metadata: options[:md], payload: "#{options[:payload]} #{number}")
  started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  if options[:kind] == "route"
    reply = client.request(message, options[:set])
    puts "request #{number}: reply #{reply.payload.inspect} metadata #{reply.metadata} after #{milliseconds.call(started)}"
  else
    client.publish(message, options[:set])
    puts "publish #{number}: ok after #{milliseconds.call(started)}"
  end
rescue => error
  puts "#{(options[:kind] == "route") ? "request" : "publish"} #{number}: #{describe.call(error)} after #{milliseconds.call(started)}"
end

if options[:stream_info]
  begin
    info = client.stream_info(target)
    if info
      puts "stream #{info.config.name.inspect}: #{info.state.messages} messages, last sequence #{info.state.last_seq}, " \
        "#{info.state.consumer_count} consumers"
    else
      puts "stream_info: the target has no stream setting"
    end
  rescue => error
    puts "stream_info: #{describe.call(error)}"
  end
end
client.close
