module Fluent
  module Plugin
    module DeadLetterQueueDropHandler

      def handle_chunk_error(tag, error, time, record)
        begin
          log.error("Dropping record from '#{tag}': error:#{error} time:#{time} record:#{record}")
        rescue=>e
          log.error("Error while trying to log and drop message from chunk '#{tag}' #{e.message}")
        end
      end

    end
  end
end
