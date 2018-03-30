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

require 'fluent/config'
require 'fluent/counter/error'
require 'fluent/plugin/storage_local'
require 'fluent/time'

module Fluent
  module Counter
    class Store
      def self.gen_key(scope, key)
        "#{scope}\t#{key}"
      end

      def initialize(opt = {})
        @log = opt[:log] || $log

        # Notice: This storage is not be implemented auto save.
        @storage = Plugin.new_storage('local', parent: DummyParent.new(@log))
        conf = if opt[:path]
                 {'persistent' => true, 'path' => opt[:path] }
               else
                 {'persistent' => false }
               end
        @storage.configure(Fluent::Config::Element.new('storage', {}, conf, []))
      end

      # This class behaves as a configurable plugin for using in storage (OwnedByMixin).
      class DummyParent
        include Configurable

        attr_reader :log

        def initialize(log)
          @log = log
        end

        def plugin_id
          'dummy_parent_store'
        end

        def plugin_id_configured?
          false
        end

        # storage_local calls PluginId#plugin_root_dir
        def plugin_root_dir
          nil
        end
      end

      def start
        @storage.load
      end

      def stop
        @storage.save
      end

      def init(key, data, ignore: false)
        ret = if v = get(key)
                raise InvalidParams.new("#{key} already exists in counter") unless ignore
                v
              else
                @storage.put(key, build_value(data))
              end

        build_response(ret)
      end

      def get(key, raise_error: false, raw: false)
        ret = if raise_error
                @storage.get(key) or raise UnknownKey.new("`#{key}` doesn't exist in counter")
              else
                @storage.get(key)
              end
        if raw
          ret
        else
          ret && build_response(ret)
        end
      end

      def key?(key)
        !!@storage.get(key)
      end

      def delete(key)
        ret = @storage.delete(key) or raise UnknownKey.new("`#{key}` doesn't exist in counter")
        build_response(ret)
      end

      def inc(key, data, force: false)
        value = data.delete('value')
        init(key, data) if !key?(key) && force
        v = get(key, raise_error: true, raw: true)
        valid_type!(v, value)

        v['total'] += value
        v['current'] += value
        t = EventTime.now
        v['last_modified_at'] = [t.sec, t.nsec]
        @storage.put(key, v)

        build_response(v)
      end

      def reset(key)
        v = get(key, raise_error: true, raw: true)
        success = false
        old_data = v.dup
        now = EventTime.now
        last_reset_at = EventTime.new(*v['last_reset_at'])

        #  Does it need reset?
        if (last_reset_at + v['reset_interval']) <= now
          success = true
          v['current'] = initial_value(v['type'])
          t = [now.sec, now.nsec]
          v['last_reset_at'] = t
          v['last_modified_at'] = t
          @storage.put(key, v)
        end

        {
          'elapsed_time' => now - last_reset_at,
          'success' => success,
          'counter_data' => build_response(old_data)
        }
      end

      private

      def build_response(d)
        {
          'name' => d['name'],
          'total' => d['total'],
          'current' => d['current'],
          'type' => d['type'],
          'reset_interval' => d['reset_interval'],
          'last_reset_at' => EventTime.new(*d['last_reset_at']),
        }
      end

      # value is Hash. value requires these fileds.
      # :name, :total, :current, :type, :reset_interval, :last_reset_at, :last_modified_at
      def build_value(data)
        type = data['type'] || 'numeric'
        now = EventTime.now
        t = [now.sec, now.nsec]

        v = initial_value(type)

        data.merge(
          'type' => type,
          'last_reset_at' => t,
          'last_modified_at' => t,
          'current' => v,
          'total' => v,
        )
      end

      def initial_value(type)
        case type
        when 'numeric', 'integer' then 0
        when 'float' then 0.0
        else raise InvalidParams.new('`type` should be integer, float, or numeric')
        end
      end

      def valid_type!(v, value)
        type = v['type']
        return unless (type != 'numeric') && (type_str(value) != type)
        raise InvalidParams.new("`type` is #{type}. You should pass #{type} value as a `value`")
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
