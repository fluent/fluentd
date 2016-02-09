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

require 'fluent/filter'
require 'fluent/plugin'

module Fluent
  class StdoutFilter < Filter
    Plugin.register_filter('stdout', self)

    # for tests
    attr_reader :formatter

    desc 'The format of the output.'
    config_param :format, :string, default: 'stdout'
    # config_param :output_type, :string, :default => 'json' (StdoutFormatter defines this)

    def configure(conf)
      super

      @formatter = Plugin.new_formatter(@format)
      @formatter.configure(conf)
    end

    def filter_stream(tag, es)
      es.each { |time, record|
        begin
          log.write @formatter.format(tag, time, record)
        rescue => e
          router.emit_error_event(tag, time, record, e)
        end
      }
      log.flush
      es
    end
  end
end
