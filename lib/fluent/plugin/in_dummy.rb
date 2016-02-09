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

require 'json'

require 'fluent/input'
require 'fluent/config/error'

module Fluent
  class DummyInput < Input
    Fluent::Plugin.register_input('dummy', self)

    BIN_NUM = 10

    desc "The value is the tag assigned to the generated events."
    config_param :tag, :string
    desc "It configures how many events to generate per second."
    config_param :rate, :integer, default: 1
    desc "If specified, each generated event has an auto-incremented key field."
    config_param :auto_increment_key, :string, default: nil
    desc "The dummy data to be generated. An array of JSON hashes or a single JSON hash."
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

      @increment_value = 0
      @dummy_index = 0
    end

    def start
      super
      @running = true
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @running = false
      @thread.join
    end

    def run
      batch_num    = (@rate / BIN_NUM).to_i
      residual_num = (@rate % BIN_NUM)
      while @running
        current_time = Time.now.to_i
        BIN_NUM.times do
          break unless (@running && Time.now.to_i <= current_time)
          wait(0.1) { emit(batch_num) }
        end
        emit(residual_num)
        # wait for next second
        while @running && Time.now.to_i <= current_time
          sleep 0.01
        end
      end
    end

    def emit(num)
      num.times { router.emit(@tag, Fluent::Engine.now, generate()) }
    end

    def generate
      d = @dummy[@dummy_index]
      unless d
        @dummy_index = 0
        d = @dummy[0]
      end
      @dummy_index += 1
      if @auto_increment_key
        d = d.dup
        d[@auto_increment_key] = @increment_value
        @increment_value += 1
      end
      d
    end

    def wait(time)
      start_time = Time.now
      yield
      sleep_time = time - (Time.now - start_time)
      sleep sleep_time if sleep_time > 0
    end
  end
end
