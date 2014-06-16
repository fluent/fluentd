#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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
module Fluent
  require 'fluent/registry'

  class TextParser
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
          raise ArgumentError, "Value must be string: #{value}"
        end

        if @cache1_key == value
          return @cache1_time
        elsif @cache2_key == value
          return @cache2_time
        else
          time = @parser.call(value).to_i
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
          config_param :types, :string, :default => nil
          config_param :types_delimiter, :string, :default => ','
          config_param :types_label_delimiter, :string, :default => ':'
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

    class RegexpParser
      include Configurable
      include TypeConverter

      config_param :time_format, :string, :default => nil

      def initialize(regexp, conf={})
        super()
        @regexp = regexp
        unless conf.empty?
          configure(conf)
        end

        @time_parser = TimeParser.new(@time_format)
        @mutex = Mutex.new
      end

      def configure(conf)
        super
        @time_parser = TimeParser.new(@time_format)
      end

      def call(text)
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
            when "time"
              time = @mutex.synchronize { @time_parser.parse(value) }
            else
              record[name] = if @type_converters.nil?
                               value
                             else
                               convert_type(name, value)
                             end
            end
          end
        }

        time ||= Engine.now

        if block_given?
          yield time, record
        else # keep backward compatibility. will be removed at v1
          return time, record
        end
      end
    end

    class JSONParser
      include Configurable

      config_param :time_key, :string, :default => 'time'
      config_param :time_format, :string, :default => nil

      def configure(conf)
        super

        unless @time_format.nil?
          @time_parser = TimeParser.new(@time_format)
          @mutex = Mutex.new
        end
      end

      def call(text)
        record = Yajl.load(text)

        if value = record.delete(@time_key)
          if @time_format
            time = @mutex.synchronize { @time_parser.parse(value) }
          else
            time = value.to_i
          end
        else
          time = Engine.now
        end

        if block_given?
          yield time, record
        else
          return time, record
        end
      rescue Yajl::ParseError
        if block_given?
          yield nil, nil
        else
          return nil, nil
        end
      end
    end

    class ValuesParser
      include Configurable
      include TypeConverter

      config_param :keys, :string
      config_param :time_key, :string, :default => nil
      config_param :time_format, :string, :default => nil

      def configure(conf)
        super

        @keys = @keys.split(",")

        if @time_key && !@keys.include?(@time_key)
          raise ConfigError, "time_key (#{@time_key.inspect}) is not included in keys (#{@keys.inspect})"
        end

        if @time_format && !@time_key
          raise ConfigError, "time_format parameter is ignored because time_key parameter is not set. at #{conf.inspect}"
        end

        @time_parser = TimeParser.new(@time_format)
        @mutex = Mutex.new
      end

      def values_map(values)
        record = Hash[keys.zip(values)]

        if @time_key
          value = record.delete(@time_key)
          time = if value.nil?
                   Engine.now
                 else
                   @mutex.synchronize { @time_parser.parse(value) }
                 end
        else
          time = Engine.now
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
    end

    class TSVParser < ValuesParser
      config_param :delimiter, :string, :default => "\t"

      def call(text)
        if block_given?
          yield values_map(text.split(@delimiter))
        else
          return values_map(text.split(@delimiter))
        end
      end
    end

    class LabeledTSVParser < ValuesParser
      config_param :delimiter,       :string, :default => "\t"
      config_param :label_delimiter, :string, :default =>  ":"
      config_param :time_key, :string, :default =>  "time"

      def configure(conf)
        conf['keys'] = conf['time_key'] || 'time'
        super(conf)
      end

      def call(text)
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

      def call(text)
        if block_given?
          yield values_map(CSV.parse_line(text))
        else
          return values_map(CSV.parse_line(text))
        end
      end
    end

    class NoneParser
      include Configurable

      config_param :message_key, :string, :default => 'message'

      def call(text)
        record = {}
        record[@message_key] = text
        if block_given?
          yield Engine.now, record
        else
          return Engine.now, record
        end
      end
    end

    class ApacheParser
      include Configurable

      REGEXP = /^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/

      def initialize
        @time_parser = TimeParser.new("%d/%b/%Y:%H:%M:%S %z")
        @mutex = Mutex.new
      end

      def call(text)
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

        if block_given?
          yield time, record
        else
          return time, record
        end
      end
    end

    class MultilineParser
      include Configurable

      config_param :format_firstline, :string, :default => nil

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

      def call(text, &block)
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
      'apache2' => Proc.new { ApacheParser.new },
      'syslog' => Proc.new { RegexpParser.new(/^(?<time>[^ ]*\s*[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[a-zA-Z0-9_\/\.\-]*)(?:\[(?<pid>[0-9]+)\])?[^\:]*\: *(?<message>.*)$/, {'time_format'=>"%b %d %H:%M:%S"}) },
      'json' => Proc.new { JSONParser.new },
      'tsv' => Proc.new { TSVParser.new },
      'ltsv' => Proc.new { LabeledTSVParser.new },
      'csv' => Proc.new { CSVParser.new },
      'nginx' => Proc.new { RegexpParser.new(/^(?<remote>[^ ]*) (?<host>[^ ]*) (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^\"]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/,  {'time_format'=>"%d/%b/%Y:%H:%M:%S %z"}) },
      'none' => Proc.new { NoneParser.new },
      'multiline' => Proc.new { MultilineParser.new },
    }.each { |name, factory|
      TEMPLATE_REGISTRY.register(name, factory)
    }

    def self.register_template(name, regexp_or_proc, time_format=nil)
      if regexp_or_proc.is_a?(Regexp)
        regexp = regexp_or_proc
        factory = Proc.new { RegexpParser.new(regexp, {'time_format'=>time_format}) }
      else
        factory = regexp_or_proc
      end

      TEMPLATE_REGISTRY.register(name, factory)
    end

    def initialize
      @parser = nil
    end

    attr_reader :parser

    def configure(conf, required=true)
      format = conf['format']

      if format == nil
        if required
          raise ConfigError, "'format' parameter is required"
        else
          return nil
        end
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

        @parser = RegexpParser.new(regexp, conf)
      else
        # built-in template
        begin
          factory = TEMPLATE_REGISTRY.lookup(format)
        rescue ConfigError => e # keep same error message
          raise ConfigError, "Unknown format template '#{format}'"
        end
        @parser = factory.call
      end

      if @parser.respond_to?(:configure)
        @parser.configure(conf)
      end

      return true
    end

    def parse(text, &block)
      if block
        @parser.call(text, &block)
      else # keep backward compatibility. Will be removed at v1
        return @parser.call(text)
      end
    end
  end
end
