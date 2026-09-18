# frozen_string_literal: true

require "service_mesh_nats"
require "service_mesh/rspec"
require_relative "support/nats_server"

RSpec.configure do |config|
  config.expect_with(:rspec) { |c| c.syntax = :expect }
  config.disable_monkey_patching!
  config.order = :random
  Kernel.srand config.seed
end
