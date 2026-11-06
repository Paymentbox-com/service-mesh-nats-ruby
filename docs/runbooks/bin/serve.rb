# frozen_string_literal: true

# Runs one endpoint or subscriber and prints each message its handler
# receives, with its metadata. For a JetStream subscriber it also prints the
# consumer's settings as the server holds them. Ctrl-C stops the runtime with
# the drain budget from --drain.
#
#   bundle exec ruby docs/runbooks/bin/serve.rb --kind route --subject demo.echo

require "json"
require "logger"
require "optparse"
require_relative "../../../lib/service_mesh_nats"

$stdout.sync = true

options = {kind: "topic", subject: "demo.created", group: nil, fail: 0, work: 0.0, drain: 10.0,
           cfg: {}, target: {}, set: {}}
pair = ->(text) { text.split("=", 2).then { |key, value| [key, value.to_s] } }
OptionParser.new do |parser|
  parser.on("--kind KIND", %w[route topic], "route for an endpoint, topic for a subscriber") { options[:kind] = _1 }
  parser.on("--subject SUBJECT", "the target's subject") { options[:subject] = _1 }
  parser.on("--group GROUP", "the consumer_group field: none or a name") { options[:group] = _1 }
  parser.on("--fail N", Integer, "fail the first N handler calls") { options[:fail] = _1 }
  parser.on("--work SECONDS", Float, "how long each handler call runs") { options[:work] = _1 }
  parser.on("--drain SECONDS", Float, "the drain budget stop gets on Ctrl-C") { options[:drain] = _1 }
  parser.on("--cfg KEY=VALUE", "a key of the config given to Client.new and Runtime.new") { options[:cfg].store(*pair.call(_1)) }
  parser.on("--target KEY=VALUE", "a key of the target's metadata") { options[:target].store(*pair.call(_1)) }
  parser.on("--set KEY=VALUE", "a key of the endpoint's or subscriber's metadata") { options[:set].store(*pair.call(_1)) }
end.parse!

config = {"url" => ENV.fetch("NATS_URL", "nats://127.0.0.1:4222"), "deployment_group" => "demo"}.merge(options[:cfg])
target = ServiceMesh::Target.new(segments: options[:subject].split("."), kind: options[:kind].to_sym, metadata: options[:target])
logger = Logger.new($stdout, formatter: ->(severity, _time, _program, message) { "#{severity} #{message}\n" })

calls = 0
lock = Mutex.new
handler = lambda do |message|
  call = lock.synchronize { calls += 1 }
  puts "#{Time.now.strftime("%H:%M:%S.%L")} call #{call}: #{message.payload.inspect}"
  message.metadata.sort.each { |key, value| puts "    #{key}: #{value}" }
  sleep options[:work] if options[:work].positive?
  raise "failing call #{call} of #{options[:fail]} on purpose" if call <= options[:fail]

  ServiceMesh::Message.new(target: target, metadata: {"Served-By" => Process.pid.to_s}, payload: "echo: #{message.payload}")
end

receivers = if options[:kind] == "route"
  {endpoints: [ServiceMesh::Endpoint.new(target: target, consumer_group: options[:group], metadata: options[:set], handler: handler)]}
else
  {subscribers: [ServiceMesh::Subscriber.new(target: target, consumer_group: options[:group], metadata: options[:set], handler: handler)]}
end

# Prints a failure the way an application would see it, and exits.
fail_with = lambda do |step, error|
  puts "#{step}: #{error.class}: #{error.message}"
  exit 1
end
begin
  client = ServiceMeshNats::Client.new(config, ServiceMesh::ServiceMap.new, logger: logger)
rescue => error
  fail_with.call("Client.new", error)
end
begin
  runtime = ServiceMeshNats::Runtime.new(client, config, logger: logger, **receivers)
  runtime.start
rescue => error
  client.close
  fail_with.call(runtime ? "start" : "Runtime.new", error)
end

receiver = runtime.receiver(target)
puts "serving #{options[:kind]} #{options[:subject]} in consumer group #{receiver.consumer_group}"
consumer = receiver.consumer_info
if consumer
  # The raw API reply, so durations show in full rather than in nats-pure's
  # whole seconds.
  reply = client.connection.request("$JS.API.CONSUMER.INFO.#{consumer.stream_name}.#{consumer.name}", "", timeout: 5)
  settings = JSON.parse(reply.data).fetch("config")
  puts "consumer #{consumer.name} on stream #{consumer.stream_name}"
  %w[ack_policy ack_wait backoff max_deliver max_ack_pending deliver_policy opt_start_seq inactive_threshold
    mem_storage num_replicas].each do |key|
    next unless settings.key?(key)

    value = settings[key]
    value = value.map { "#{_1 / 1e9}s" } if key == "backoff"
    value = "#{value / 1e9}s" if %w[ack_wait inactive_threshold].include?(key)
    puts "    #{key}: #{value}"
  end
end

stop = Queue.new
trap("INT") { stop << true }
stop.pop
puts "stopping with a drain of #{options[:drain]}s"
puts "stop returned #{runtime.stop(options[:drain])}"
