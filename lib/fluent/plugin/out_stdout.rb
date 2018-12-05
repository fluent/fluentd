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
  class StdoutOutput < Output
    Fluent::Plugin.register_output('stdout', self)

    helpers :inject, :formatter, :compat_parameters

    DEFAULT_LINE_FORMAT_TYPE = 'stdout'
    DEFAULT_FORMAT_TYPE = 'json'
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S.%9N %z'

    config_section :buffer do
      config_set_default :chunk_keys, ['tag']
      config_set_default :flush_at_shutdown, true
      config_set_default :chunk_limit_size, 10 * 1024
    end

    config_section :format do
      config_set_default :@type, DEFAULT_LINE_FORMAT_TYPE
      config_set_default :output_type, DEFAULT_FORMAT_TYPE
    end

    def prefer_buffered_processing
      false
    end

    def prefer_delayed_commit
      @delayed
    end

    attr_accessor :delayed
    attr_accessor :formatter

    def initialize
      super
      @delayed = false
    end

    def configure(conf)
      compat_parameters_convert(conf, :inject, :formatter)

      super

      @formatter = formatter_create
    end

    def process(tag, es)
      es.each {|time,record|
        $log.write(format(tag, time, record))
      }
      $log.flush
    end

    def format(tag, time, record)
      record = inject_values_to_record(tag, time, record)
      @formatter.format(tag, time, record).chomp + "\n"
    end

    def write(chunk)
      chunk.write_to($log)
    end

    def try_write(chunk)
      chunk.write_to($log)
      commit_write(chunk.unique_id)
    end
  end
end
