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

    helpers :formatter, :compat_parameters, :inject

    DEFAULT_FORMAT_TYPE = 'stdout'

    config_section :format do
      config_set_default :@type, DEFAULT_FORMAT_TYPE
    end

    # for tests
    attr_reader :formatter

    def configure(conf)
      compat_parameters_convert(conf, :inject, :formatter)
      super
      @formatter = formatter_create
    end

    def filter_stream(tag, es)
      es.each { |time, record|
        begin
          r = inject_values_to_record(tag, time, record)
          log.write @formatter.format(tag, time, r)
        rescue => e
          router.emit_error_event(tag, time, record, e)
        end
      }
      log.flush
      es
    end
  end
end
