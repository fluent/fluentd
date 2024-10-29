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

require 'fluent/plugin/output'

module Fluent::Plugin
  class BufferOutput < Output
    Fluent::Plugin.register_output("buffer", self)
    helpers :event_emitter

    desc "Try to remove the buffer directory after terminate." +
         " Mainly for the --with-source-only feature."
    config_param :cleanup_after_shutdown, :bool, default: false

    config_section :buffer do
      config_set_default :@type, "file"
      config_set_default :chunk_keys, ["tag"]
      config_set_default :flush_mode, :interval
      config_set_default :flush_interval, 10
    end

    def multi_workers_ready?
      true
    end

    def initialize
      super

      @buffer_path = nil
    end

    def configure(conf)
      super

      raise Fluent::ConfigError, "The buffer plugin does not be supported. It must have 'buffer_path' getter. You can use 'file' buffer." unless @buffer.respond_to?(:buffer_path)

      @buffer_path = @buffer.buffer_path
    end

    def write(chunk)
      return if chunk.empty?
      router.emit_stream(chunk.metadata.tag, Fluent::MessagePackEventStream.new(chunk.read))
    end

    def terminate
      super

      return unless cleanup_after_shutdown

      unless Dir.empty?(@buffer_path)
        if @buffer_config.flush_at_shutdown
          log.warn "some buffer files remain in #{@buffer_path}, so cancel cleanup the directory." +
                   " Please consider saving or recovering the buffer files in the directory."
        end
        return
      end

      begin
        FileUtils.remove_dir(@buffer_path)
      rescue => e
        log.warn "failed to remove the empty buffer dirctory: #{@buffer_path}", error: e
      end
    end
  end
end
