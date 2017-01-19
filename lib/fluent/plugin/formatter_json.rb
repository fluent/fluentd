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

require 'fluent/plugin/formatter'
require 'fluent/env'

module Fluent
  module Plugin
    class JSONFormatter < Formatter
      Plugin.register_formatter('json', self)

      config_param :json_parser, :string, default: 'oj'
      config_param :add_newline, :bool, default: true

      def configure(conf)
        super

        begin
          raise LoadError unless @json_parser == 'oj'
          require 'oj'
          Oj.default_options = Fluent::DEFAULT_OJ_OPTIONS
          @dump_proc = Oj.method(:dump)
        rescue LoadError
          @dump_proc = Yajl.method(:dump)
        end

        # format json is used on various highload environment, so re-define method to skip if check
        unless @add_newline
          define_singleton_method(:format, method(:format_without_nl))
        end
      end

      def format(tag, time, record)
        "#{@dump_proc.call(record)}\n"
      end

      def format_without_nl(tag, time, record)
        @dump_proc.call(record)
      end
    end
  end
end
