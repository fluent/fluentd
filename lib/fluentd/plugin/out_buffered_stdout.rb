require_relative './object_buffered_output'
require 'yajl'

module Fluentd
  module Plugin

    class BufferedStdout < ObjectBufferedOutput
      Plugin.register_output('buffered_stdout', self)

      OUTPUT_PROCS = {
        :json => Proc.new {|record| Yajl.dump(record) },
        :hash => Proc.new {|record| record.to_s },
      }

      config_param :status_log_interval, :time, :default => nil

      config_param :output_type, :default => :json do |val|
        case val.downcase
        when 'json'
          :json
        when 'hash'
          :hash
        else
          raise ConfigError, "stdout output output_type should be 'json' or 'hash'"
        end
      end

      def configure(conf)
        super
        @output_proc = OUTPUT_PROCS[@output_type]
      end

      def start
        @running = true
        @logs = 0
        if @status_log_interval
          actor.every(@status_log_interval) do
            next unless @running
            Fluentd.log.info "output records: #{@logs}"
          end
        end

        super
      end

      def stop
        @running = false
        super
      end

      def write_objects(tag, chunk)
        chunk.msgpack_each do |time, record|
          @logs += 1
          $stdout.write "#{Time.at(time).localtime} #{tag}: #{@output_proc.call(record)}\n"
        end
      end
    end
  end
end
