# frozen_string_literal: true

require "service_mesh"

require_relative "service_mesh_nats/version"
require_relative "service_mesh_nats/errors"
require_relative "service_mesh_nats/log"
require_relative "service_mesh_nats/config"
require_relative "service_mesh_nats/target"
require_relative "service_mesh_nats/header"
require_relative "service_mesh_nats/consumer_api"
require_relative "service_mesh_nats/jetstream"
require_relative "service_mesh_nats/pull_loop"
require_relative "service_mesh_nats/client"
require_relative "service_mesh_nats/runtime"
require_relative "service_mesh_nats/runtime_public"
require_relative "service_mesh_nats/runtime_receivers"
require_relative "service_mesh_nats/runtime_dispatch"
require_relative "service_mesh_nats/runtime_serve"
require_relative "service_mesh_nats/runtime_jetstream"
require_relative "service_mesh_nats/consumers"

# NATS transport for the Service Mesh API Specification. The contract types
# and errors come from the service_mesh gem.
module ServiceMeshNats
end
