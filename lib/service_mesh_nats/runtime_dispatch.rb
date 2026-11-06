# frozen_string_literal: true

require "concurrent"

module ServiceMeshNats
  class Runtime
    private

    def accepting?
      @accept_lock.synchronize { @accepting }
    end

    def accepting=(on)
      @accept_lock.synchronize { @accepting = on }
    end

    # Runs the block on +pool+, bounded by the runtime's permits. +pool+ is
    # the pool of the start that opened the receiver. Returns false, and does
    # not run the block, for a message arriving after the runtime stopped
    # accepting or after that pool shut down, so a receiver left over from a
    # failed start runs no handlers once a later start accepts again.
    #
    # A core message waits here for a permit. A JetStream message arrives
    # with +permit_held+, because its pull loop takes the permit to size its
    # pulls. The permit is returned when the block finishes, or right away
    # when it does not run.
    def dispatch(pool, permit_held: false, &block)
      unless permit_held
        until @permits.try_acquire(1, PERMIT_POLL)
          return false unless accepting? && pool.running?
        end
      end

      @accept_lock.synchronize do
        unless @accepting && pool.running?
          @permits.release
          return false
        end

        pool.post do
          block.call
        ensure
          @permits.release
        end
      end
      true
    rescue Concurrent::RejectedExecutionError
      @permits.release
      false
    end
  end
end
