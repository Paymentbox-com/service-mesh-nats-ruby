# frozen_string_literal: true

require "service_mesh"

require_relative "service_mesh_nats/version"
require_relative "service_mesh_nats/errors"
require_relative "service_mesh_nats/settings"
require_relative "service_mesh_nats/subject"
require_relative "service_mesh_nats/client"
require_relative "service_mesh_nats/runtime"

# NATS transport for the Service Mesh API Specification. The contract types
# and errors come from the service_mesh gem.
module ServiceMeshNats
end
