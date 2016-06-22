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

require 'fluent/config/error'
require 'fluent/compat/record_filter_mixin'
require 'fluent/time'
require 'fluent/timezone'

module Fluent
  module Compat
    module SetTimeKeyMixin
      include RecordFilterMixin

      attr_accessor :include_time_key, :time_key, :localtime, :timezone

      def configure(conf)
        @include_time_key = false
        @localtime = false
        @timezone = nil

        super

        if s = conf['include_time_key']
          include_time_key = Fluent::Config.bool_value(s)
          raise Fluent::ConfigError, "Invalid boolean expression '#{s}' for include_time_key parameter" if include_time_key.nil?

          @include_time_key = include_time_key
        end

        if @include_time_key
          @time_key     = conf['time_key'] || 'time'
          @time_format  = conf['time_format']

          if    conf['localtime']
            @localtime = true
          elsif conf['utc']
            @localtime = false
          end

          if conf['timezone']
            @timezone = conf['timezone']
            Fluent::Timezone.validate!(@timezone)
          end

          @timef = Fluent::TimeFormatter.new(@time_format, @localtime, @timezone)
        end
      end

      def filter_record(tag, time, record)
        super

        record[@time_key] = @timef.format(time) if @include_time_key
      end
    end
  end
end
