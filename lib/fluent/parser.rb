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

require 'time'
require 'json'

require 'yajl'

require 'fluent/config/error'
require 'fluent/config/element'
require 'fluent/configurable'
require 'fluent/env'
require 'fluent/engine'
require 'fluent/registry'
require 'fluent/time'

module Fluent
  class ParserError < StandardError
  end

  class Parser
    include Configurable

    # SET false BEFORE CONFIGURE, to return nil when time not parsed
    # 'configure()' may raise errors for unexpected configurations
    attr_accessor :estimate_current_event

    config_param :keep_time_key, :bool, default: false

    def initialize
      super
      @estimate_current_event = true
    end

    def configure(conf)
      super
    end

    def parse(text)
      raise NotImplementedError, "Implement this method in child class"
    end

    # Keep backward compatibility for existing plugins
    def call(*a, &b)
      parse(*a, &b)
    end
  end

  class TextParser
    # Keep backward compatibility for existing plugins
    ParserError = ::Fluent::ParserError

    class TimeParser
      def initialize(time_format)
        @cache1_key = nil
        @cache1_time = nil
        @cache2_key = nil
        @cache2_time = nil
        @parser =
          if time_format
            Proc.new { |value| Time.strptime(value, time_format) }
          else
            Time.method(:parse)
          end
      end

      def parse(value)
        unless value.is_a?(String)
          raise ParserError, "value must be string: #{value}"
        end

        if @cache1_key == value
          return @cache1_time
        elsif @cache2_key == value
          return @cache2_time
        else
          begin
            time = @parser.call(value).to_i
          rescue => e
            raise ParserError, "invalid time format: value = #{value}, error_class = #{e.class.name}, error = #{e.message}"
          end
          @cache1_key = @cache2_key
          @cache1_time = @cache2_time
          @cache2_key = value
          @cache2_time = time
          return time
        end
      end
    end

    module TypeConverter
      Converters = {
        'string' => lambda { |v| v.to_s },
        'integer' => lambda { |v| v.to_i },
        'float' => lambda { |v| v.to_f },
        'bool' => lambda { |v|
          case v.downcase
          when 'true', 'yes', '1'
            true
          else
            false
          end
        },
        'time' => lambda { |v, time_parser|
          time_parser.parse(v)
        },
        'array' => lambda { |v, delimiter|
          v.to_s.split(delimiter)
        }
      }

      def self.included(klass)
        klass.instance_eval {
          config_param :types, :string, default: nil
          config_param :types_delimiter, :string, default: ','
          config_param :types_label_delimiter, :string, default: ':'
        }
      end

      def configure(conf)
        super

        @type_converters = parse_types_parameter unless @types.nil?
      end

      private

      def convert_type(name, value)
        converter = @type_converters[name]
        converter.nil? ? value : converter.call(value)
      end

      def parse_types_parameter
        converters = {}

        @types.split(@types_delimiter).each { |pattern_name|
          name, type, format = pattern_name.split(@types_label_delimiter, 3)
          raise ConfigError, "Type is needed" if type.nil?

          case type
          when 'time'
            t_parser = TimeParser.new(format)
            converters[name] = lambda { |v|
              Converters[type].call(v, t_parser)
            }
          when 'array'
            delimiter = format || ','
            converters[name] = lambda { |v|
              Converters[type].call(v, delimiter)
            }
          else
            converters[name] = Converters[type]
          end
        }

        converters
      end
    end

    class RegexpParser < Parser
      include TypeConverter

      config_param :time_key, :string, default: 'time'
      config_param :time_format, :string, default: nil

      def initialize(regexp, conf={})
        super()
        @regexp = regexp
        unless conf.empty?
          conf = Config::Element.new('default_regexp_conf', '', conf, []) unless conf.is_a?(Config::Element)
          configure(conf)
        end

        @time_parser = TimeParser.new(@time_format)
        @mutex = Mutex.new
      end

      def configure(conf)
        super
        @time_parser = TimeParser.new(@time_format)
      end

      def patterns
        {'format' => @regexp, 'time_format' => @time_format}
      end

      def parse(text)
        m = @regexp.match(text)
        unless m
          if block_given?
            yield nil, nil
            return
          else
            return nil, nil
          end
        end

        time = nil
        record = {}

        m.names.each {|name|
          if value = m[name]
            case name
            when @time_key
              time = @mutex.synchronize { @time_parser.parse(value) }
              if @keep_time_key
                record[name] = if @type_converters.nil?
                                 value
                               else
                                 convert_type(name, value)
                               end
              end
            else
              record[name] = if @type_converters.nil?
                               value
                             else
                               convert_type(name, value)
                             end
            end
          end
        }

        if @estimate_current_event
          time ||= Engine.now
        end

        if block_given?
          yield time, record
        else # keep backward compatibility. will be removed at v1
          return time, record
        end
      end
    end

    class JSONParser < Parser
      config_param :time_key, :string, default: 'time'
      config_param :time_format, :string, default: nil
      config_param :json_parser, :string, default: 'oj'

      def configure(conf)
        super

        unless @time_format.nil?
          @time_parser = TimeParser.new(@time_format)
          @mutex = Mutex.new
        end

        begin
          raise LoadError unless @json_parser == 'oj'
          require 'oj'
          Oj.default_options = Fluent::DEFAULT_OJ_OPTIONS
          @load_proc = Oj.method(:load)
          @error_class = Oj::ParseError
        rescue LoadError
          @load_proc = Yajl.method(:load)
          @error_class = Yajl::ParseError
        end
      end

      def parse(text)
        record = @load_proc.call(text)

        value = @keep_time_key ? record[@time_key] : record.delete(@time_key)
        if value
          if @time_format
            time = @mutex.synchronize { @time_parser.parse(value) }
          else
            begin
              time = value.to_i
            rescue => e
              raise ParserError, "invalid time value: value = #{value}, error_class = #{e.class.name}, error = #{e.message}"
            end
          end
        else
          if @estimate_current_event
            time = Engine.now
          else
            time = nil
          end
        end

        if block_given?
          yield time, record
        else
          return time, record
        end
      rescue @error_class
        if block_given?
          yield nil, nil
        else
          return nil, nil
        end
      end
    end

    class ValuesParser < Parser
      include TypeConverter

      config_param :keys, default: [] do |val|
        if val.start_with?('[') # This check is enough because keys parameter is simple. No '[' started column name.
          JSON.load(val)
        else
          val.split(",")
        end
      end
      config_param :time_key, :string, default: nil
      config_param :time_format, :string, default: nil
      config_param :null_value_pattern, :string, default: nil
      config_param :null_empty_string, :bool, default: false

      def configure(conf)
        super

        if @time_key && !@keys.include?(@time_key) && @estimate_current_event
          raise ConfigError, "time_key (#{@time_key.inspect}) is not included in keys (#{@keys.inspect})"
        end

        if @time_format && !@time_key
          raise ConfigError, "time_format parameter is ignored because time_key parameter is not set. at #{conf.inspect}"
        end

        @time_parser = TimeParser.new(@time_format)

        if @null_value_pattern
          @null_value_pattern = Regexp.new(@null_value_pattern)
        end

        @mutex = Mutex.new
      end

      def values_map(values)
        record = Hash[keys.zip(values.map { |value| convert_value_to_nil(value) })]

        if @time_key
          value = @keep_time_key ? record[@time_key] : record.delete(@time_key)
          time = if value.nil?
                   if @estimate_current_event
                     Engine.now
                   else
                     nil
                   end
                 else
                   @mutex.synchronize { @time_parser.parse(value) }
                 end
        elsif @estimate_current_event
          time = Engine.now
        else
          time = nil
        end

        convert_field_type!(record) if @type_converters

        return time, record
      end

      private

      def convert_field_type!(record)
        @type_converters.each_key { |key|
          if value = record[key]
            record[key] = convert_type(key, value)
          end
        }
      end

      def convert_value_to_nil(value)
        if value and @null_empty_string
          value = (value == '') ? nil : value
        end
        if value and @null_value_pattern
          value = ::Fluent::StringUtil.match_regexp(@null_value_pattern, value) ? nil : value
        end
        value
      end
    end

    class TSVParser < ValuesParser
      config_param :delimiter, :string, default: "\t"

      def configure(conf)
        super
        @key_num = @keys.length
      end

      def parse(text)
        if block_given?
          yield values_map(text.split(@delimiter, @key_num))
        else
          return values_map(text.split(@delimiter, @key_num))
        end
      end
    end

    class LabeledTSVParser < ValuesParser
      config_param :delimiter,       :string, default: "\t"
      config_param :label_delimiter, :string, default: ":"
      config_param :time_key, :string, default: "time"

      def configure(conf)
        conf['keys'] = conf['time_key'] || 'time'
        super(conf)
      end

      def parse(text)
        @keys  = []
        values = []

        text.split(delimiter).each do |pair|
          key, value = pair.split(label_delimiter, 2)
          @keys.push(key)
          values.push(value)
        end

        if block_given?
          yield values_map(values)
        else
          return values_map(values)
        end
      end
    end

    class CSVParser < ValuesParser
      def initialize
        super
        require 'csv'
      end

      def parse(text)
        if block_given?
          yield values_map(CSV.parse_line(text))
        else
          return values_map(CSV.parse_line(text))
        end
      end
    end

    class NoneParser < Parser
      config_param :message_key, :string, default: 'message'

      def parse(text)
        record = {}
        record[@message_key] = text
        time = @estimate_current_event ? Engine.now : nil
        if block_given?
          yield time, record
        else
          return time, record
        end
      end
    end

    class ApacheParser < Parser
      REGEXP = /^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>(?:[^\"]|\\.)*?)(?: +\S*)?)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>(?:[^\"]|\\.)*)" "(?<agent>(?:[^\"]|\\.)*)")?$/
      TIME_FORMAT = "%d/%b/%Y:%H:%M:%S %z"

      def initialize
        super
        @time_parser = TimeParser.new(TIME_FORMAT)
        @mutex = Mutex.new
      end

      def patterns
        {'format' => REGEXP, 'time_format' => TIME_FORMAT}
      end

      def parse(text)
        m = REGEXP.match(text)
        unless m
          if block_given?
            yield nil, nil
            return
          else
            return nil, nil
          end
        end

        host = m['host']
        host = (host == '-') ? nil : host

        user = m['user']
        user = (user == '-') ? nil : user

        time = m['time']
        time = @mutex.synchronize { @time_parser.parse(time) }

        method = m['method']
        path = m['path']

        code = m['code'].to_i
        code = nil if code == 0

        size = m['size']
        size = (size == '-') ? nil : size.to_i

        referer = m['referer']
        referer = (referer == '-') ? nil : referer

        agent = m['agent']
        agent = (agent == '-') ? nil : agent

        record = {
          "host" => host,
          "user" => user,
          "method" => method,
          "path" => path,
          "code" => code,
          "size" => size,
          "referer" => referer,
          "agent" => agent,
        }
        record["time"] = m['time'] if @keep_time_key

        if block_given?
          yield time, record
        else
          return time, record
        end
      end
    end

    class SyslogParser < Parser
      # From existence TextParser pattern
      REGEXP = /^(?<time>[^ ]*\s*[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[a-zA-Z0-9_\/\.\-]*)(?:\[(?<pid>[0-9]+)\])?(?:[^\:]*\:)? *(?<message>.*)$/
      # From in_syslog default pattern
      REGEXP_WITH_PRI = /^\<(?<pri>[0-9]+)\>(?<time>[^ ]* {1,2}[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[a-zA-Z0-9_\/\.\-]*)(?:\[(?<pid>[0-9]+)\])?(?:[^\:]*\:)? *(?<message>.*)$/
      REGEXP_RFC5424 = /\A^\<(?<pri>[0-9]{1,3})\>[1-9]\d{0,2} (?<time>[^ ]+) (?<host>[^ ]+) (?<ident>[^ ]+) (?<pid>[-0-9]+) (?<msgid>[^ ]+) (?<extradata>(\[(.*)\]|[^ ])) (?<message>.+)$\z/
      REGEXP_DETECT_RFC5424 = /^\<.*\>[1-9]\d{0,2}/

      config_param :time_format, :string, default: "%b %d %H:%M:%S"
      config_param :with_priority, :bool, default: false
      config_param :message_format, :enum, list: [:rfc3164, :rfc5424, :auto], default: :rfc3164
      config_param :rfc5424_time_format, :string, default: "%Y-%m-%dT%H:%M:%S.%L%z"

      def initialize
        super
        @mutex = Mutex.new
      end

      def configure(conf)
        super

        @time_parser_rfc3164 = @time_parser_rfc5424 = nil
        @regexp = case @message_format
                  when :rfc3164
                    class << self
                      alias_method :parse, :parse_plain
                    end
                    @with_priority ? REGEXP_WITH_PRI : REGEXP
                  when :rfc5424
                    class << self
                      alias_method :parse, :parse_plain
                    end
                    REGEXP_RFC5424
                  when :auto
                    class << self
                      alias_method :parse, :parse_auto
                    end
                    @time_parser_rfc3164 = TextParser::TimeParser.new(@time_format)
                    @time_parser_rfc5424 = TextParser::TimeParser.new(@rfc5424_time_format)
                    nil
                  end
        @time_parser = TextParser::TimeParser.new(@time_format)
      end

      def patterns
        {'format' => @regexp, 'time_format' => @time_format}
      end

      def parse(text)
        # This is overwritten in configure
      end

      def parse_auto(text, &block)
        if REGEXP_DETECT_RFC5424.match(text)
          @regexp = REGEXP_RFC5424
          @time_parser = @time_parser_rfc5424
        else
          @regexp = @with_priority ? REGEXP_WITH_PRI : REGEXP
          @time_parser = @time_parser_rfc3164
        end
        parse_plain(text, &block)
      end

      def parse_plain(text, &block)
        m = @regexp.match(text)
        unless m
          if block_given?
            yield nil, nil
            return
          else
            return nil, nil
          end
        end

        time = nil
        record = {}

        m.names.each { |name|
          if value = m[name]
            case name
            when "pri"
              record['pri'] = value.to_i
            when "time"
              time = @mutex.synchronize { @time_parser.parse(value.gsub(/ +/, ' ')) }
              record[name] = value if @keep_time_key
            else
              record[name] = value
            end
          end
        }

        if @estimate_current_event
          time ||= Engine.now
        end

        if block_given?
          yield time, record
        else
          return time, record
        end
      end
    end

    class MultilineParser < Parser
      config_param :format_firstline, :string, default: nil

      FORMAT_MAX_NUM = 20

      def configure(conf)
        super

        formats = parse_formats(conf).compact.map { |f| f[1..-2] }.join
        begin
          @regex = Regexp.new(formats, Regexp::MULTILINE)
          if @regex.named_captures.empty?
            raise "No named captures"
          end
          @parser = RegexpParser.new(@regex, conf)
        rescue => e
          raise ConfigError, "Invalid regexp '#{formats}': #{e}"
        end

        if @format_firstline
          check_format_regexp(@format_firstline, 'format_firstline')
          @firstline_regex = Regexp.new(@format_firstline[1..-2])
        end
      end

      def parse(text, &block)
        if block
          @parser.call(text, &block)
        else
          @parser.call(text)
        end
      end

      def has_firstline?
        !!@format_firstline
      end

      def firstline?(text)
        @firstline_regex.match(text)
      end

      private

      def parse_formats(conf)
        check_format_range(conf)

        prev_format = nil
        (1..FORMAT_MAX_NUM).map { |i|
          format = conf["format#{i}"]
          if (i > 1) && prev_format.nil? && !format.nil?
            raise ConfigError, "Jump of format index found. format#{i - 1} is missing."
          end
          prev_format = format
          next if format.nil?

          check_format_regexp(format, "format#{i}")
          format
        }
      end

      def check_format_range(conf)
        invalid_formats = conf.keys.select { |k|
          m = k.match(/^format(\d+)$/)
          m ? !((1..FORMAT_MAX_NUM).include?(m[1].to_i)) : false
        }
        unless invalid_formats.empty?
          raise ConfigError, "Invalid formatN found. N should be 1 - #{FORMAT_MAX_NUM}: " + invalid_formats.join(",")
        end
      end

      def check_format_regexp(format, key)
        if format[0] == '/' && format[-1] == '/'
          begin
            Regexp.new(format[1..-2], Regexp::MULTILINE)
          rescue => e
            raise ConfigError, "Invalid regexp in #{key}: #{e}"
          end
        else
          raise ConfigError, "format should be Regexp, need //, in #{key}: '#{format}'"
        end
      end
    end

    TEMPLATE_REGISTRY = Registry.new(:config_type, 'fluent/plugin/parser_')
    {
      'apache' => Proc.new { RegexpParser.new(/^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/, {'time_format'=>"%d/%b/%Y:%H:%M:%S %z"}) },
      'apache_error' => Proc.new { RegexpParser.new(/^\[[^ ]* (?<time>[^\]]*)\] \[(?<level>[^\]]*)\](?: \[pid (?<pid>[^\]]*)\])?( \[client (?<client>[^\]]*)\])? (?<message>.*)$/) },
      'apache2' => Proc.new { ApacheParser.new },
      'syslog' => Proc.new { SyslogParser.new },
      'json' => Proc.new { JSONParser.new },
      'tsv' => Proc.new { TSVParser.new },
      'ltsv' => Proc.new { LabeledTSVParser.new },
      'csv' => Proc.new { CSVParser.new },
      'nginx' => Proc.new { RegexpParser.new(/^(?<remote>[^ ]*) (?<host>[^ ]*) (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^\"]*?)(?: +\S*)?)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/,  {'time_format'=>"%d/%b/%Y:%H:%M:%S %z"}) },
      'none' => Proc.new { NoneParser.new },
      'multiline' => Proc.new { MultilineParser.new },
    }.each { |name, factory|
      TEMPLATE_REGISTRY.register(name, factory)
    }

    def self.register_template(name, regexp_or_proc, time_format=nil)
      if regexp_or_proc.is_a?(Class)
        factory = Proc.new { regexp_or_proc.new }
      elsif regexp_or_proc.is_a?(Regexp)
        regexp = regexp_or_proc
        factory = Proc.new { RegexpParser.new(regexp, {'time_format'=>time_format}) }
      else
        factory = regexp_or_proc
      end

      TEMPLATE_REGISTRY.register(name, factory)
    end

    def self.lookup(format)
      if format.nil?
        raise ConfigError, "'format' parameter is required"
      end

      if format[0] == ?/ && format[format.length-1] == ?/
        # regexp
        begin
          regexp = Regexp.new(format[1..-2])
          if regexp.named_captures.empty?
            raise "No named captures"
          end
        rescue
          raise ConfigError, "Invalid regexp '#{format[1..-2]}': #{$!}"
        end

        RegexpParser.new(regexp)
      else
        # built-in template
        begin
          factory = TEMPLATE_REGISTRY.lookup(format)
        rescue ConfigError => e # keep same error message
          raise ConfigError, "Unknown format template '#{format}'"
        end

        factory.call
      end
    end

    def initialize
      @parser = nil
      @estimate_current_event = nil
    end

    attr_reader :parser

    # SET false BEFORE CONFIGURE, to return nil when time not parsed
    # 'configure()' may raise errors for unexpected configurations
    attr_accessor :estimate_current_event

    def configure(conf, required=true)
      format = conf['format']

      @parser = TextParser.lookup(format)
      if ! @estimate_current_event.nil? && @parser.respond_to?(:'estimate_current_event=')
        @parser.estimate_current_event = @estimate_current_event
      end

      if @parser.respond_to?(:configure)
        @parser.configure(conf)
      end

      return true
    end

    def parse(text, &block)
      if block
        @parser.parse(text, &block)
      else # keep backward compatibility. Will be removed at v1
        return @parser.parse(text)
      end
    end
  end
end
