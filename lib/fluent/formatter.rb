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

require 'fluent/configurable'
require 'fluent/env'
require 'fluent/registry'
require 'fluent/mixin'

module Fluent
  class Formatter
    include Configurable

    def configure(conf)
      super
    end

    def format(tag, time, record)
      raise NotImplementedError, "Implement this method in child class"
    end
  end

  module TextFormatter
    module HandleTagAndTimeMixin
      def self.included(klass)
        klass.instance_eval {
          config_param :include_time_key, :bool, default: false
          config_param :time_key, :string, default: 'time'
          config_param :time_format, :string, default: nil
          config_param :include_tag_key, :bool, default: false
          config_param :tag_key, :string, default: 'tag'
          config_param :localtime, :bool, default: true
          config_param :timezone, :string, default: nil
        }
      end

      def configure(conf)
        super

        if conf['utc']
          @localtime = false
        end
        @timef = TimeFormatter.new(@time_format, @localtime, @timezone)
      end

      def filter_record(tag, time, record)
        if @include_tag_key
          record[@tag_key] = tag
        end
        if @include_time_key
          record[@time_key] = @timef.format(time)
        end
      end
    end

    class OutFileFormatter < Formatter
      include HandleTagAndTimeMixin

      config_param :output_time, :bool, default: true
      config_param :output_tag, :bool, default: true
      config_param :delimiter, default: "\t" do |val|
        case val
        when /SPACE/i then ' '
        when /COMMA/i then ','
        else "\t"
        end
      end

      def format(tag, time, record)
        filter_record(tag, time, record)
        header = ''
        header << "#{@timef.format(time)}#{@delimiter}" if @output_time
        header << "#{tag}#{@delimiter}" if @output_tag
        "#{header}#{Yajl.dump(record)}\n"
      end
    end

    class StdoutFormatter < Formatter
      config_param :output_type, :string, default: 'json'

      def configure(conf)
        super

        @formatter = Plugin.new_formatter(@output_type)
        @formatter.configure(conf)
      end

      def format(tag, time, record)
        header = "#{Time.now.localtime} #{tag}: "
        "#{header}#{@formatter.format(tag, time, record)}"
      end
    end

    module StructuredFormatMixin
      def self.included(klass)
        klass.instance_eval {
          config_param :time_as_epoch, :bool, default: false
        }
      end

      def configure(conf)
        super

        if @time_as_epoch
          if @include_time_key
            @include_time_key = false
          else
            $log.warn "include_time_key is false so ignore time_as_epoch"
            @time_as_epoch = false
          end
        end
      end

      def format(tag, time, record)
        filter_record(tag, time, record)
        record[@time_key] = time if @time_as_epoch
        format_record(record)
      end
    end

    class JSONFormatter < Formatter
      include HandleTagAndTimeMixin
      include StructuredFormatMixin

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

      def format_record(record)
        "#{@dump_proc.call(record)}\n"
      end

      def format_without_nl(tag, time, record)
        @dump_proc.call(record)
      end
    end

    class HashFormatter < Formatter
      include HandleTagAndTimeMixin
      include StructuredFormatMixin

      config_param :add_newline, :bool, default: true

      def format_record(record)
        if @add_newline
          "#{record.to_s}\n"
        else
          record.to_s
        end
      end
    end

    class MessagePackFormatter < Formatter
      include HandleTagAndTimeMixin
      include StructuredFormatMixin

      def format_record(record)
        record.to_msgpack
      end
    end

    class LabeledTSVFormatter < Formatter
      include HandleTagAndTimeMixin

      config_param :delimiter, :string, default: "\t"
      config_param :label_delimiter, :string, default: ":"
      config_param :add_newline, :bool, default: true

      def format(tag, time, record)
        filter_record(tag, time, record)
        formatted = record.inject('') { |result, pair|
          result << @delimiter if result.length.nonzero?
          result << "#{pair.first}#{@label_delimiter}#{pair.last}"
        }
        formatted << "\n".freeze if @add_newline
        formatted
      end
    end

    class CsvFormatter < Formatter
      include HandleTagAndTimeMixin

      config_param :delimiter, default: ',' do |val|
        ['\t', 'TAB'].include?(val) ? "\t" : val
      end
      config_param :force_quotes, :bool, default: true
      config_param :fields, :array, value_type: :string
      config_param :add_newline, :bool, default: true

      def initialize
        super
        require 'csv'
      end

      def configure(conf)
        super
        @fields = fields.select { |f| !f.empty? }
        raise ConfigError, "empty value is specified in fields parameter" if @fields.empty?

        @generate_opts = {col_sep: @delimiter, force_quotes: @force_quotes}
      end

      def format(tag, time, record)
        filter_record(tag, time, record)
        row = @fields.inject([]) do |memo, key|
            memo << record[key]
            memo
        end
        line = CSV.generate_line(row, @generate_opts)
        line.chomp! unless @add_newline
        line
      end
    end

    class SingleValueFormatter < Formatter
      config_param :message_key, :string, default: 'message'
      config_param :add_newline, :bool, default: true

      def format(tag, time, record)
        text = record[@message_key].to_s.dup
        text << "\n" if @add_newline
        text
      end
    end

    class ProcWrappedFormatter < Formatter
      def initialize(proc)
        @proc = proc
      end

      def configure(conf)
      end

      def format(tag, time, record)
        @proc.call(tag, time, record)
      end
    end

    TEMPLATE_REGISTRY = Registry.new(:formatter_type, 'fluent/plugin/formatter_')
    {
      'out_file' => Proc.new { OutFileFormatter.new },
      'stdout' => Proc.new { StdoutFormatter.new },
      'json' => Proc.new { JSONFormatter.new },
      'hash' => Proc.new { HashFormatter.new },
      'msgpack' => Proc.new { MessagePackFormatter.new },
      'ltsv' => Proc.new { LabeledTSVFormatter.new },
      'csv' => Proc.new { CsvFormatter.new },
      'single_value' => Proc.new { SingleValueFormatter.new },
    }.each { |name, factory|
      TEMPLATE_REGISTRY.register(name, factory)
    }

    def self.register_template(name, factory_or_proc)
      factory = if factory_or_proc.is_a?(Class) # XXXFormatter
                  Proc.new { factory_or_proc.new }
                elsif factory_or_proc.arity == 3 # Proc.new { |tag, time, record| }
                  Proc.new { ProcWrappedFormatter.new(factory_or_proc) }
                else # Proc.new { XXXFormatter.new }
                  factory_or_proc
                end

      TEMPLATE_REGISTRY.register(name, factory)
    end

    def self.lookup(format)
      TEMPLATE_REGISTRY.lookup(format).call
    end

    # Keep backward-compatibility
    def self.create(conf)
      format = conf['format']
      if format.nil?
        raise ConfigError, "'format' parameter is required"
      end

      formatter = lookup(format)
      if formatter.respond_to?(:configure)
        formatter.configure(conf)
      end
      formatter
    end
  end
end
