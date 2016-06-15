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
  class BufferedStdoutOutput < Output
    Fluent::Plugin.register_output('buffered_stdout', self)

    desc 'Output format.(json,hash)'
    config_param :output_type, default: 'json'
    config_section :buffer do
      config_set_default :chunk_keys, ['tag']
      config_set_default :flush_at_shutdown, true
      config_set_default :chunk_limit_size, 10 * 1024
    end

    attr_accessor :delayed

    def initialize
      super
      @delayed = false
    end

    def prefer_delayed_commit
      @delayed
    end

    def configure(conf)
      super
      @formatter = Fluent::Plugin.new_formatter(@output_type, parent: self)
      @formatter.configure(conf)
    end

    def write(chunk)
      chunk.write_to($log)
    end

    def try_write(chunk)
      chunk.write_to($log)
    end

    def format(tag, time, record)
      "#{Time.at(time).localtime} #{tag}: #{@formatter.format(tag, time, record).chomp}\n"
    end
  end
end
