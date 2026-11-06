# frozen_string_literal: true

require "nats/io/client"

module ServiceMeshNats
  # Converts between message metadata and NATS headers.
  module Header
    # Copies +metadata+ into a NATS header, one string value per key, or nil
    # when it is empty.
    def self.to_header(metadata)
      metadata.empty? ? nil : metadata.transform_keys(&:to_s).transform_values(&:to_s)
    end

    # to_header for a message being sent. ServiceMesh::MESSAGE_ID_KEY and
    # Nats-Msg-Id are one field, and ServiceMesh::MESSAGE_ID_KEY wins.
    def self.outgoing_header(metadata)
      header = to_header(metadata)
      header[NATS::JetStream::Header::MSG_ID] = header[ServiceMesh::MESSAGE_ID_KEY] if header&.key?(ServiceMesh::MESSAGE_ID_KEY)
      header
    end

    # The metadata handed to a handler, never nil. Nats-Msg-Id and
    # ServiceMesh::MESSAGE_ID_KEY are one field, and Nats-Msg-Id wins.
    def self.incoming_metadata(header)
      metadata = header.to_h.dup
      message_id = metadata[NATS::JetStream::Header::MSG_ID]
      metadata[ServiceMesh::MESSAGE_ID_KEY] = message_id if message_id
      metadata
    end
  end
  private_constant :Header
end
