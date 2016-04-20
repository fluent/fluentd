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

require 'fluent/plugin/base'
require 'fluent/plugin/owned_by_mixin'

require 'fluent/mixin' # for TimeFormatter

module Fluent
  module Plugin
    class Formatter < Base
      include OwnedByMixin

      configured_in :format

      def format(tag, time, record)
        raise NotImplementedError, "Implement this method in child class"
      end

      # Mixins for formatter plugins
      module HandleTagAndTimeMixin
        def self.included(klass)
          klass.instance_eval {
            config_param :include_time_key, :bool, default: false
            config_param :time_key, :string, default: 'time'
            config_param :time_format, :string, default: nil
            config_param :include_tag_key, :bool, default: false
            config_param :tag_key, :string, default: 'tag'
            config_param :localtime, :bool, default: true
            config_param :timezone, :string, default: nil
          }
        end

        def configure(conf)
          super

          if conf['utc']
            @localtime = false
          end
          @timef = Fluent::TimeFormatter.new(@time_format, @localtime, @timezone)
        end

        def filter_record(tag, time, record)
          if @include_tag_key
            record[@tag_key] = tag
          end
          if @include_time_key
            record[@time_key] = @timef.format(time)
          end
        end
      end

      module StructuredFormatMixin
        def self.included(klass)
          klass.instance_eval {
            config_param :time_as_epoch, :bool, default: false
          }
        end

        def configure(conf)
          super

          if @time_as_epoch
            if @include_time_key
              @include_time_key = false
            else
              log.warn "include_time_key is false so ignore time_as_epoch"
              @time_as_epoch = false
            end
          end
        end

        def format(tag, time, record)
          filter_record(tag, time, record)
          record[@time_key] = time if @time_as_epoch
          format_record(record)
        end
      end
    end

    class ProcWrappedFormatter < Formatter
      def initialize(proc)
        super()
        @proc = proc
      end

      def format(tag, time, record)
        @proc.call(tag, time, record)
      end
    end
  end
end
