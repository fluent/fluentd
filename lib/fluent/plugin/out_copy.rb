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

require 'fluent/plugin/multi_output'
require 'fluent/config/error'
require 'fluent/event'

module Fluent::Plugin
  class CopyOutput < MultiOutput
    Fluent::Plugin.register_output('copy', self)

    desc 'If true, pass different record to each `store` plugin.'
    config_param :deep_copy, :bool, default: false, deprecated: "use 'copy_mode' parameter instead"
    desc 'Pass different record to each `store` plugin by specified method'
    config_param :copy_mode, :enum, list: [:no_copy, :shallow, :deep, :marshal], default: :no_copy

    attr_reader :ignore_errors, :ignore_if_prev_successes

    def initialize
      super
      @ignore_errors = []
      @ignore_if_prev_successes = []
    end

    def configure(conf)
      super

      @copy_proc = gen_copy_proc
      @stores.each_with_index { |store, i|
        if i == 0 && store.arg.include?('ignore_if_prev_success')
          raise Fluent::ConfigError, "ignore_if_prev_success must specify 2nd or later <store> directives"
        end
        @ignore_errors << (store.arg.include?('ignore_error'))
        @ignore_if_prev_successes << (store.arg.include?('ignore_if_prev_success'))
      }
      if @ignore_errors.uniq.size == 1 && @ignore_errors.include?(true) && !@ignore_if_prev_successes.include?(true)
        log.warn "ignore_errors are specified in all <store>, but ignore_if_prev_success is not specified. Is this intended?"
      end
    end

    def multi_workers_ready?
      true
    end

    def process(tag, es)
      unless es.repeatable?
        m = Fluent::MultiEventStream.new
        es.each {|time,record|
          m.add(time, record)
        }
        es = m
      end
      success = Array.new(outputs.size)
      outputs.each_with_index do |output, i|
        begin
          if i > 0 && success[i - 1] && @ignore_if_prev_successes[i]
            log.debug "ignore copy because prev_success in #{output.plugin_id}", index: i
          else
            output.emit_events(tag, @copy_proc ? @copy_proc.call(es) : es)
            success[i] = true
          end
        rescue => e
          if @ignore_errors[i]
            log.error "ignore emit error in #{output.plugin_id}", error: e
          else
            raise e
          end
        end
      end
    end

    private

    def gen_copy_proc
      @copy_mode = :shallow if @deep_copy

      case @copy_mode
      when :no_copy
         nil
      when :shallow
        Proc.new { |es| es.dup }
      when :deep
        Proc.new { |es|
          packer = Fluent::MessagePackFactory.msgpack_packer
          times = []
          records = []
          es.each { |time, record|
            times << time
            packer.pack(record)
          }
          Fluent::MessagePackFactory.msgpack_unpacker.feed_each(packer.full_pack) { |record|
            records << record
          }
          Fluent::MultiEventStream.new(times, records)
        }
      when :marshal
        Proc.new { |es|
          new_es = Fluent::MultiEventStream.new
          es.each { |time, record|
            new_es.add(time, Marshal.load(Marshal.dump(record)))
          }
          new_es
        }
      end
    end
  end
end
