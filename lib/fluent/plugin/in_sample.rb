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

require 'fluent/plugin/input'
require 'fluent/config/error'

module Fluent::Plugin
  class SampleInput < Input
    Fluent::Plugin.register_input('sample', self)
    Fluent::Plugin.register_input('dummy', self)

    helpers :thread, :storage

    BIN_NUM = 10
    DEFAULT_STORAGE_TYPE = 'local'

    desc "The value is the tag assigned to the generated events."
    config_param :tag, :string
    desc "The number of events in event stream of each emits."
    config_param :size, :integer, default: 1
    desc "It configures how many events to generate per second."
    config_param :rate, :integer, default: 1
    desc "If specified, each generated event has an auto-incremented key field."
    config_param :auto_increment_key, :string, default: nil
    desc "The boolean to suspend-and-resume incremental value after restart"
    config_param :suspend, :bool, default: false,deprecated: 'This parameters is ignored'
    desc "Reuse the sample data to reduce the load when sending large amounts of data. You can enable it if filter does not do destructive change"
    config_param :reuse_record, :bool, default: false
    desc "The sample data to be generated. An array of JSON hashes or a single JSON hash."
    config_param :sample, alias: :dummy, default: [{"message" => "sample"}] do |val|
      begin
        parsed = JSON.parse(val)
      rescue JSON::ParserError => ex
        # Fluent::ConfigParseError, "got incomplete JSON" will be raised
        # at literal_parser.rb with --use-v1-config, but I had to
        # take care at here for the case of --use-v0-config.
        raise Fluent::ConfigError, "#{ex.class}: #{ex.message}"
      end
      sample = parsed.is_a?(Array) ? parsed : [parsed]
      sample.each_with_index do |e, i|
        raise Fluent::ConfigError, "#{i}th element of sample, #{e}, is not a hash" unless e.is_a?(Hash)
      end
      sample
    end

    def initialize
      super
      @storage = nil
    end

    def configure(conf)
      super
      @sample_index = 0
      config = conf.elements.find{|e| e.name == 'storage' }
      @storage = storage_create(usage: 'suspend', conf: config, default_type: DEFAULT_STORAGE_TYPE)
    end

    def multi_workers_ready?
      true
    end

    def start
      super

      @storage.put(:increment_value, 0) unless @storage.get(:increment_value)
      # keep 'dummy' to avoid breaking changes for existing environment. Change it in fluentd v2
      @storage.put(:dummy_index, 0) unless @storage.get(:dummy_index)

      if @auto_increment_key && !@storage.get(:auto_increment_value)
        @storage.put(:auto_increment_value, -1)
      end

      thread_create(:sample_input, &method(:run))
    end

    def run
      batch_num    = (@rate / BIN_NUM).to_i
      residual_num = (@rate % BIN_NUM)
      while thread_current_running?
        current_time = Time.now.to_i
        BIN_NUM.times do
          break unless (thread_current_running? && Time.now.to_i <= current_time)
          wait(0.1) { emit(batch_num) }
        end
        emit(residual_num) if thread_current_running?
        # wait for next second
        while thread_current_running? && Time.now.to_i <= current_time
          sleep 0.01
        end
      end
    end

    def emit(num)
      begin
        if @size > 1
          num.times do
            router.emit_array(@tag, Array.new(@size) { [Fluent::EventTime.now, generate] })
          end
        else
          num.times { router.emit(@tag, Fluent::EventTime.now, generate) }
        end
      rescue => _
        # ignore all errors not to stop emits by emit errors
      end
    end

    def next_sample
      d = @reuse_record ? @sample[@sample_index] : @sample[@sample_index].dup
      @sample_index += 1
      return d if d

      @sample_index = 0
      next_sample
    end

    def generate
      d = next_sample
      if @auto_increment_key
        d = d.dup if @reuse_record
        d[@auto_increment_key] = @storage.update(:auto_increment_value){|v| v + 1 }
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
