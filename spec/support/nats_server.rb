# frozen_string_literal: true

require "socket"
require "timeout"
require "tmpdir"
require "fileutils"

# Runs a real nats-server with JetStream on a free loopback port for one
# example group. JetStream stores its data in a temporary directory that is
# removed when the server stops.
#
#   let(:url) { nats_server_url }
#
# The binary is found on PATH or at NATS_SERVER_BIN.
#
# A client that connects through +nats_proxy+ can be disconnected from the
# server without stopping it.
module NatsServerHelper
  BIN = ENV.fetch("NATS_SERVER_BIN") do
    found = ENV.fetch("PATH", "").split(File::PATH_SEPARATOR).map { |d| File.join(d, "nats-server") }.find { |p| File.executable?(p) }
    found || "/opt/homebrew/bin/nats-server"
  end

  class Server
    attr_reader :url, :port

    # Starts the server, with JetStream unless +jetstream+ is false.
    def initialize(jetstream: true)
      @port = free_port
      @url = "nats://127.0.0.1:#{@port}"
      @store_dir = Dir.mktmpdir("nats-js-")
      arguments = ["-a", "127.0.0.1", "-p", @port.to_s]
      arguments += ["-js", "-sd", @store_dir] if jetstream
      @pid = Process.spawn(BIN, *arguments, out: File::NULL, err: File::NULL)
      wait_ready(@port)
    end

    # Suspends the server process for +seconds+, so it sends nothing while
    # its clients stay connected.
    def freeze(seconds)
      Process.kill("STOP", @pid)
      sleep seconds
    ensure
      Process.kill("CONT", @pid)
    end

    def stop
      Process.kill("TERM", @pid)
      Process.wait(@pid)
    rescue Errno::ESRCH, Errno::ECHILD
      nil
    ensure
      FileUtils.rm_rf(@store_dir)
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

  # Forwards each client connection to the server's port, and can drop them
  # all while refusing new ones.
  class Proxy
    def initialize(server_port)
      @server_port = server_port
      @listener = TCPServer.new("127.0.0.1", 0)
      @sockets = []
      @lock = Mutex.new
      @cut = false
      @thread = Thread.new { accept_connections }
    end

    def url
      "nats://127.0.0.1:#{@listener.addr[1]}"
    end

    # Closes every connection through the proxy and refuses new ones while
    # the block runs.
    def cut
      @lock.synchronize do
        @cut = true
        close_sockets
      end
      yield
    ensure
      @lock.synchronize { @cut = false }
    end

    def stop
      @listener.close
      @thread.join
      @lock.synchronize { close_sockets }
    end

    private

    def accept_connections
      loop do
        client = @listener.accept
        @lock.synchronize do
          if @cut
            client.close
          else
            server = TCPSocket.new("127.0.0.1", @server_port)
            @sockets.push(client, server)
            forward(client, server)
            forward(server, client)
          end
        end
      end
    rescue IOError, SystemCallError
      nil
    end

    def forward(from, to)
      Thread.new do
        loop { to.write(from.readpartial(65_536)) }
      rescue IOError, SystemCallError
        [from, to].each(&:close)
      end
    end

    def close_sockets
      @sockets.each(&:close)
      @sockets.clear
    end
  end

  def self.included(group)
    group.class_eval do
      before(:all) { @nats_server = Server.new }
      after(:all) { @nats_server.stop }
      after do
        (@nats_proxies || []).each(&:stop)
        @plain_nats_server&.stop
      end
    end
  end

  def nats_server_url
    @nats_server.url
  end

  # A config for the server with a short request_timeout, so a request or
  # publish that never gets an answer fails quickly. +extra+ is merged over
  # it.
  def nats_config(extra = {})
    {"url" => nats_server_url, "request_timeout" => "2"}.merge(extra)
  end

  def freeze_nats_server(seconds)
    @nats_server.freeze(seconds)
  end

  # The URL of a second server, without JetStream, stopped after the
  # example.
  def nats_server_without_jetstream_url
    (@plain_nats_server ||= Server.new(jetstream: false)).url
  end

  # A Proxy to the server, stopped after the example.
  def nats_proxy
    proxy = Proxy.new(@nats_server.port)
    (@nats_proxies ||= []) << proxy
    proxy
  end
end

RSpec.configure do |config|
  config.include NatsServerHelper, :nats
end
