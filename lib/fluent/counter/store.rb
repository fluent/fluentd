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

require 'fluent/time'
require 'fluent/counter/error'

module Fluent
  module Counter
    class Store
      Value = Struct.new(:name, :total, :current, :type, :reset_interval, :last_reset_at, :last_modified_at) do
        class << self
          def init(data)
            type = data['type'] || 'numeric'
            now = EventTime.now
            v = initial_value(type)
            Value.new(data['name'], v, v, type, data['reset_interval'], now, now)
          end

          def initial_value(type)
            case type
            when 'numeric', 'integer' then 0
            when 'float' then 0.0
            else raise InvalidParams.new('`type` should be integer, float, or numeric')
            end
          end
        end

        def to_response_hash
          {
            'name' => name,
            'total' => total,
            'current' => current,
            'type' => type,
            'reset_interval' => reset_interval,
            'last_reset_at' => last_reset_at,
          }
        end
      end

      def self.gen_key(scope, key)
        "#{scope}\t#{key}"
      end

      def initialize
        @store = {}
      end

      def init(name, scope, data, ignore: false)
        if v = get(name, scope)
          raise InvalidParams.new("#{name} already exists in counter") unless ignore
          v
        else
          key = Store.gen_key(scope, name)
          @store[key] = Value.init(data)
        end
      end

      def get(name, scope, raise_error: false)
        key = Store.gen_key(scope, name)
        if raise_error
          @store[key] or raise UnknownKey.new("`#{name}` doesn't exist in counter")
        else
          @store[key]
        end
      end

      def key?(name, scope)
        key = Store.gen_key(scope, name)
        @store.key?(key)
      end

      def delete(name, scope)
        key = Store.gen_key(scope, name)
        @store.delete(key) or raise UnknownKey.new("`#{name}` doesn't exist in counter")
      end

      def inc(name, scope, data, force: false)
        init(name, scope, data) if force
        v = get(name, scope, raise_error: true)
        value = data['value']
        valid_type!(v, value)

        v.total += value
        v.current += value
        v.last_modified_at = EventTime.now
        v
      end

      def reset(name, scope)
        v = get(name, scope, raise_error: true)
        now = EventTime.now
        success = false
        old_data = v.to_response_hash

        #  Does it need reset?
        if (v.last_reset_at + v.reset_interval) <= now
          success = true
          v.current = Value.initial_value(v.type)
          v.last_reset_at = now
          v.last_modified_at = now
        end

        {
          'elapsed_time' => now - old_data['last_reset_at'],
          'success' => success,
          'counter_data' => old_data
        }
      end

      private

      def valid_type!(v, value)
        return unless (v.type != 'numeric') && (type_str(value) != v.type)
        raise InvalidParams.new("`type` is #{v.type}. You should pass #{v.type} value as a `value`")
      end

      def type_str(v)
        case v
        when Integer
          'integer'
        when Float
          'float'
        when Numeric
          'numeric'
        else
          raise InvalidParams.new("`type` should be integer, float, or numeric")
        end
      end
    end
  end
end
