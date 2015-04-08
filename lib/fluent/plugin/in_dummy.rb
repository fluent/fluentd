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

require 'fluent/plugin/input'
require 'fluent/plugin_support/thread'
require 'fluent/plugin_support/storage'

module Fluent::Plugin
  class DummyInput < Fluent::Plugin::Input
    include Fluent::PluginSupport::Thread
    include Fluent::PluginSupport::Storage

    Fluent::Plugin.register_input('dummy', self)

    BIN_NUM = 10

    config_param :suspend, :bool, default: false
    config_param :tag, :string
    config_param :rate, :integer, default: 1
    config_param :auto_increment_key, :string, default: nil
    config_param :dummy, default: [{"message"=>"dummy"}] do |val|
      begin
        parsed = JSON.parse(val)
      rescue JSON::ParserError => e
        # Fluent::ConfigParseError, "got incomplete JSON" will be raised
        # at literal_parser.rb with --use-v1-config, but I had to
        # take care at here for the case of --use-v0-config.
        raise Fluent::ConfigError, "#{e.class}: #{e.message}"
      end
      dummy = parsed.is_a?(Array) ? parsed : [parsed]
      dummy.each_with_index do |e, i|
        raise Fluent::ConfigError, "#{i}th element of dummy, #{e}, is not a hash" unless e.is_a?(Hash)
      end
      dummy
    end

    def configure(conf)
      super

      unless @suspend
        storage.autosave = false
        storage.save_at_shutdown = false
      end
    end

    def start
      super

      storage.put(:increment_value, 0) unless storage.get(:increment_value)
      storage.put(:dummy_index, 0) unless storage.get(:dummy_index)

      batch_num    = (@rate / BIN_NUM).to_i
      residual_num = (@rate % BIN_NUM)

      thread_create do
        while thread_current_running?
          current_time = Time.now.to_i
          BIN_NUM.times do
            break unless (thread_current_running? && Time.now.to_i <= current_time)
            wait(0.1) { emit(batch_num) }
          end
          emit(residual_num)
          # wait for next second
          while thread_current_running? && Time.now.to_i <= current_time
            sleep 0.01
          end
        end
      end
    end

    def emit(num)
      num.times { router.emit(@tag, Fluent::Engine.now, generate()) }
    end

    def generate
      storage.synchronize do
        index = storage.get(:dummy_index)
        d = @dummy[index]
        unless d
          index = 0
          d = @dummy[index]
        end
        storage.put(:dummy_index, index + 1)

        if @auto_increment_key
          d = d.dup
          inc_value = storage.get(:increment_value)
          d[@auto_increment_key] = inc_value
          storage.put(:increment_value, inc_value + 1)
        end

        d
      end
    end

    def wait(time)
      start_time = Time.now
      yield
      sleep_time = time - (Time.now - start_time)
      sleep sleep_time if sleep_time > 0
    end
  end
end
