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

require 'fluent/output'

module Fluent
  class BufferedStdoutOutput < BufferedOutput
    Plugin.register_output('buffered_stdout', self)

    config_param :output_type, :default => 'json'

    def configure(conf)
      super
      @formatter = Plugin.new_formatter(@output_type)
      @formatter.configure(conf)
    end

    def write(chunk)
      chunk.msgpack_each do |time, tag, record|
        log.write "#{time} #{tag}: #{@formatter.format(time.localtime.to_s, tag, record)}\n"
      end
      log.flush
    end
  end
end
