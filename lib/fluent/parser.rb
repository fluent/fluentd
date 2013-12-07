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

    class RegexpParser
      include Configurable

      config_param :time_format, :string, :default => nil
      config_param :name_separator, :string, :default => '|'

      Converters = {
        'string' => lambda { |v| v.to_s },
        'integer' => lambda { |v| v.to_i },
        'float' => lambda { |v| v.to_f },
        'bool' => lambda { |v|
          case v
          when 'true', 'yes'
            true
          else
            false
          end
        },
        'time' => lambda { |v, time_parser|
          time_parser.parse(v)
        },
        'array' => lambda { |v, separator|
          v.split(separator)
        }
      }

      def initialize(regexp, conf={})
        super()
        @regexp = regexp
        unless conf.empty?
          configure(conf)
        end

        @converters, names = parse_pattern_names(regexp)
        @regexp = if @converters.empty?
                    regexp
                  else
                    re = generate_regexp(regexp, names)
                    $log.debug "format regex changed to #{re.source}"
                    re
                  end
        @time_parser = TimeParser.new(@time_format)
        @mutex = Mutex.new
      end

      def call(text)
        m = @regexp.match(text)
        unless m
          $log.warn "pattern not match: #{text.inspect}"
          return nil, nil
        end

        time = nil
        record = {}

        m.names.each {|name|
          if value = m[name]
            case name
            when "time"
              time = @mutex.synchronize { @time_parser.parse(value) }
            else
              converter = @converters[name]
              if converter.nil?
                record[name] = value
              else
                record[name] = converter.call(value)
              end
            end
          end
        }

        time ||= Engine.now

        return time, record
      end

      private

      def parse_pattern_names(regexp)
        converters = {}
        names = []

        regexp.names.each { |pattern_name|
          name, type, format = pattern_name.split(@name_separator, 3)
          names << name
          next if type.nil?

          case type
          when 'time'
            t_parser = TimeParser.new(format)
            converters[name] = lambda { |v|
              Converters[type].call(v, t_parser)
            }
          when 'array'
            separator = format || ','
            converters[name] = lambda { |v|
              Converters[type].call(v, separator)
            }
          else
            converters[name] = Converters[type]
          end
        }

        return converters, names
      end

      def generate_regexp(regexp, names)
        i = 0
        source = regexp.source.gsub(Regexp.new('\(\?<.+?>')) { |re|
          name = names[i]
          i += 1
          "(?<#{name}>"
        }
        Regexp.compile(source)
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

        return time, record
      rescue Yajl::ParseError
        $log.warn "pattern not match: #{text.inspect}: #{$!}"
        return nil, nil
      end
    end

    class ValuesParser
      include Configurable

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
          time = @mutex.synchronize { @time_parser.parse(value) }
        else
          time = Engine.now
        end

        return time, record
      end
    end

    class TSVParser < ValuesParser
      config_param :delimiter, :string, :default => "\t"

      def call(text)
        return values_map(text.split(@delimiter))
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

        return values_map(values)
      end
    end

    class CSVParser < ValuesParser
      def initialize
        super
        require 'csv'
      end

      def call(text)
        return values_map(CSV.parse_line(text))
      end
    end

    class NoneParser
      include Configurable

      config_param :message_key, :string, :default => 'message'

      def call(text)
        record = {}
        record[@message_key] = text
        return Engine.now, record
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
          $log.warn "pattern not match: #{text.inspect}"
          return nil, nil
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

        return time, record
      end
    end

    TEMPLATE_FACTORIES = {
      'apache' => Proc.new { RegexpParser.new(/^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/, {'time_format'=>"%d/%b/%Y:%H:%M:%S %z"}) },
      'apache2' => Proc.new { ApacheParser.new },
      'syslog' => Proc.new { RegexpParser.new(/^(?<time>[^ ]*\s*[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[a-zA-Z0-9_\/\.\-]*)(?:\[(?<pid>[0-9]+)\])?[^\:]*\: *(?<message>.*)$/, {'time_format'=>"%b %d %H:%M:%S"}) },
      'json' => Proc.new { JSONParser.new },
      'tsv' => Proc.new { TSVParser.new },
      'ltsv' => Proc.new { LabeledTSVParser.new },
      'csv' => Proc.new { CSVParser.new },
      'nginx' => Proc.new { RegexpParser.new(/^(?<remote>[^ ]*) (?<host>[^ ]*) (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^\"]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/,  {'time_format'=>"%d/%b/%Y:%H:%M:%S %z"}) },
      'none' => Proc.new { NoneParser.new },
    }

    def self.register_template(name, regexp_or_proc, time_format=nil)
      if regexp_or_proc.is_a?(Regexp)
        regexp = regexp_or_proc
        factory = Proc.new { RegexpParser.new(regexp, {'time_format'=>time_format}) }
      else
        factory = regexp_or_proc
      end

      TEMPLATE_FACTORIES[name] = factory
    end

    def initialize
      @parser = nil
    end

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
        factory = TEMPLATE_FACTORIES[format]
        unless factory
          raise ConfigError, "Unknown format template '#{format}'"
        end
        @parser = factory.call
      end

      if @parser.respond_to?(:configure)
        @parser.configure(conf)
      end

      return true
    end

    def parse(text)
      return @parser.call(text)
    end
  end
end
