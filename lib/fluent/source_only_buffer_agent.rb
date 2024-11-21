#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'fluent/agent'
require 'fluent/system_config'

module Fluent
  class SourceOnlyBufferAgent < Agent
    # Use INSTANCE_ID to use the same base dir as the other workers.
    # This will make recovery easier.
    BUFFER_DIR_NAME = Fluent::INSTANCE_ID

    def initialize(log:, system_config:)
      super(log: log)

      @default_buffer_path = File.join(system_config.root_dir || DEFAULT_BACKUP_DIR, 'source-only-buffer', BUFFER_DIR_NAME)
      @optional_buffer_config = system_config.source_only_buffer.to_h.transform_keys(&:to_s)
      @base_buffer_dir = nil
      @actual_buffer_dir = nil
    end

    def configure(flush: false)
      buffer_config = @optional_buffer_config.compact
      buffer_config['flush_at_shutdown'] = flush ? 'true' : 'false'
      buffer_config['flush_thread_count'] = 0 unless flush
      buffer_config['path'] ||= @default_buffer_path

      super(
        Config::Element.new('SOURCE_ONLY_BUFFER', '', {}, [
          Config::Element.new('match', '**', {'@type' => 'buffer', '@label' => '@ROOT'}, [
            Config::Element.new('buffer', '', buffer_config, [])
          ])
        ])
      )

      @base_buffer_dir = buffer_config['path']
      # It can be "#{@base_buffer_dir}/worker#{fluentd_worker_id}/" when using multiple workers
      @actual_buffer_dir = File.dirname(outputs[0].buffer.path)

      unless flush
        log.info "with-source-only: the emitted data will be stored in the buffer files under" +
                 " #{@base_buffer_dir}. You can send SIGWINCH to the supervisor process to cancel" +
                 " with-source-only mode and process data."
      end
    end

    def cleanup
      unless (Dir.empty?(@actual_buffer_dir) rescue true)
        log.warn "some buffer files remain in #{@base_buffer_dir}." +
                 " Please consider recovering or saving the buffer files in the directory." +
                 " To recover them, you can set the buffer path manually to system config and" +
                 " retry, i.e., restart Fluentd with with-source-only mode and send SIGWINCH again." +
                 " Config Example:\n#{config_example_to_recover(@base_buffer_dir)}"
        return
      end

      begin
        FileUtils.remove_dir(@base_buffer_dir)
      rescue Errno::ENOENT
        # This worker doesn't need to do anything. Another worker may remove the dir first.
      rescue => e
        log.warn "failed to remove the buffer directory: #{@base_buffer_dir}", error: e
      end
    end

    def emit_error_event(tag, time, record, error)
      error_info = {error: error, location: (error.backtrace ? error.backtrace.first : nil), tag: tag, time: time, record: record}
      log.warn "SourceOnlyBufferAgent: dump an error event:", error_info
    end

    def handle_emits_error(tag, es, error)
      error_info = {error: error, location: (error.backtrace ? error.backtrace.first : nil), tag: tag}
      log.warn "SourceOnlyBufferAgent: emit transaction failed:", error_info
      log.warn_backtrace
    end

    private

    def config_example_to_recover(path)
      <<~EOC
        <system>
          <source_only_buffer>
            path #{path}
          </source_only_buffer>
        </system>
      EOC
    end
  end
end
