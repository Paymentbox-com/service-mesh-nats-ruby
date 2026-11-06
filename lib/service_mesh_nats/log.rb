# frozen_string_literal: true

module ServiceMeshNats
  # Formats log lines. Ruby's Logger takes one string, so the fields that the
  # Go transport passes as slog attributes follow the message as key=value
  # pairs, under the same names. Fields whose value is nil are left out.
  module Log
    def self.line(message, **fields)
      pairs = fields.compact.map { |key, value| "#{key}=#{value}" }
      (["service_mesh_nats: #{message}"] + pairs).join(" ")
    end
  end
  private_constant :Log
end
