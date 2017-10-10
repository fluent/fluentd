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
      # See "Example Custom Assertion: https://test-unit.github.io/test-unit/en/Test/Unit/Assertions.html
      def assert_equal_event_time(expected, actual, message = nil)
        expected_s = "#{Time.at(expected.sec)} (nsec #{expected.nsec})"
        actual_s   = "#{Time.at(actual.sec)  } (nsec #{actual.nsec})"
        message = build_message(message, <<EOT, expected_s, actual_s)
<?> expected but was
<?>.
EOT
        assert_block(message) do
          expected.is_a?(Fluent::EventTime) && actual.is_a?(Fluent::EventTime) && expected.sec == actual.sec && expected.nsec == actual.nsec
        end
      end

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

      def with_timezone(tz)
        oldtz, ENV['TZ'] = ENV['TZ'], tz
        yield
      ensure
        ENV['TZ'] = oldtz
      end

      def with_worker_config(root_dir: nil, workers: nil, worker_id: nil, &block)
        if workers
          if worker_id
            if worker_id >= workers
              raise "worker_id must be between 0 and (workers - 1)"
            end
          else
            worker_id = 0
          end
        end

        opts = {}
        opts['root_dir'] = root_dir if root_dir
        opts['workers'] = workers if workers

        ENV['SERVERENGINE_WORKER_ID'] = worker_id.to_s
        Fluent::SystemConfig.overwrite_system_config(opts, &block)
      ensure
        ENV.delete('SERVERENGINE_WORKER_ID')
      end

      def time2str(time, localtime: false, format: nil)
        if format
          if localtime
            Time.at(time).strftime(format)
          else
            Time.at(time).utc.strftime(format)
          end
        else
          if localtime
            Time.at(time).iso8601
          else
            Time.at(time).utc.iso8601
          end
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

      #
      # Use this method with v0.12 compatibility layer.
      #
      # For v0.14 API, use `driver.logs` instead.
      #
      def capture_log(driver)
        tmp = driver.instance.log.out
        driver.instance.log.out = StringIO.new
        yield
        return driver.instance.log.out.string
      ensure
        driver.instance.log.out = tmp
      end

      def capture_stdout
        out = StringIO.new
        $stdout = out
        yield
        out.string.force_encoding('utf-8')
      ensure
        $stdout = STDOUT
      end
    end
  end
end
