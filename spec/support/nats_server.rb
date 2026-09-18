# frozen_string_literal: true

require "socket"
require "timeout"

# Runs a real nats-server on a free loopback port for one example group.
#
#   let(:url) { nats_server_url }
#
# The binary is found on PATH or at NATS_SERVER_BIN.
module NatsServerHelper
  BIN = ENV.fetch("NATS_SERVER_BIN") do
    found = ENV.fetch("PATH", "").split(File::PATH_SEPARATOR).map { |d| File.join(d, "nats-server") }.find { |p| File.executable?(p) }
    found || "/opt/homebrew/bin/nats-server"
  end

  class Server
    attr_reader :url

    def initialize
      port = free_port
      @url = "nats://127.0.0.1:#{port}"
      @pid = Process.spawn(BIN, "-a", "127.0.0.1", "-p", port.to_s, out: File::NULL, err: File::NULL)
      wait_ready(port)
    end

    def stop
      Process.kill("TERM", @pid)
      Process.wait(@pid)
    rescue Errno::ESRCH, Errno::ECHILD
      nil
    end

    private

    def free_port
      TCPServer.open("127.0.0.1", 0) { |s| s.addr[1] }
    end

    def wait_ready(port)
      Timeout.timeout(5) do
        TCPSocket.new("127.0.0.1", port).close
      rescue Errno::ECONNREFUSED
        sleep 0.02
        retry
      end
    end
  end

  def self.included(group)
    group.class_eval do
      before(:all) { @nats_server = Server.new }
      after(:all) { @nats_server.stop }
    end
  end

  def nats_server_url
    @nats_server.url
  end
end

RSpec.configure do |config|
  config.include NatsServerHelper, :nats
end
