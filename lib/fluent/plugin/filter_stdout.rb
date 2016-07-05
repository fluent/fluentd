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

require 'fluent/plugin/filter'

module Fluent::Plugin
  class StdoutFilter < Filter
    Fluent::Plugin.register_filter('stdout', self)

    helpers :formatter

    # for tests
    attr_reader :formatter

    def configure(conf)
      super
    end

    def start
      @formatter = if !@formatter_configs.empty?
                     formatter_create(conf: @formatter_configs)
                   else
                     # For v0.12 flat style parameter
                     format = @config["format"] || "stdout"
                     conf = {}
                     conf[:output_type] = @config["output_type"] || "json"
                     conf[:include_time_key] = @config["include_time_key"]
                     formatter_create(type: format, conf: conf)
                   end
      super
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
