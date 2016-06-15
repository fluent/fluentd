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

require 'fluent/config/element'
require 'fluent/msgpack_factory'
require 'fluent/time'

module Fluent
  module Test
    module Helpers
      def config_element(name = 'test', argument = '', params = {}, elements = [])
        Fluent::Config::Element.new(name, argument, params, elements)
      end

      def event_time(str=nil, format: nil)
        if str
          if format
            Fluent::EventTime.from_time(Time.strptime(str, format))
          else
            Fluent::EventTime.parse(str)
          end
        else
          Fluent::EventTime.now
        end
      end

      def msgpack(type)
        case type
        when :factory
          Fluent::MessagePackFactory.factory
        when :packer
          Fluent::MessagePackFactory.packer
        when :unpacker
          Fluent::MessagePackFactory.unpacker
        else
          raise ArgumentError, "unknown msgpack object type '#{type}'"
        end
      end
    end
  end
end
