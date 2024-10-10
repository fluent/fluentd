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

require 'securerandom'

require 'fluent/agent'
require 'fluent/system_config'

module Fluent
  class SourceOnlyBufferAgent < Agent
    BUFFER_DIR_NAME = SecureRandom.uuid

    def initialize(log:, system_config:)
      super(log: log)

      @buffer_path = File.join(system_config.root_dir || DEFAULT_BACKUP_DIR, 'source-only-buffer', BUFFER_DIR_NAME)
    end

    def configure(flush: false)
      super(
        Config::Element.new('SOURCE_ONLY_BUFFER', '', {}, [
          Config::Element.new('match', '**', {'@type' => 'buffer', '@label' => '@ROOT', 'cleanup_after_shutdown' => 'true'}, [
            Config::Element.new('buffer', '', {
              'path' => @buffer_path,
              'flush_at_shutdown' => flush ? 'true' : 'false',
              'flush_thread_count' => flush ? 1 : 0,
              'overflow_action' => "drop_oldest_chunk",
            }, [])
          ])
        ])
      )
    end
  end
end
