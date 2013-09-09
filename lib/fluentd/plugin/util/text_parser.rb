#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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

module Fluentd
  module Plugin
    module Util

      require 'fluentd/configurable'
      require 'yajl'

      class TextParser
        class RegexpParser
          include Configurable

          config_param :time_format, :string, :default => nil

          def initialize(regexp)
            super()
            @regexp = regexp
          end

          def call(text)
            m = @regexp.match(text)
            unless m
              Fluentd.log.warn "pattern not match: #{text.inspect}"
              return nil, nil
            end

            time = nil
            record = {}

            m.names.each {|name|
              if value = m[name]
                case name
                when "time"
                  if @time_format
                    time = Time.strptime(value, @time_format).to_i
                  else
                    time = Time.parse(value).to_i
                  end
                else
                  record[name] = value
                end
              end
            }

            return time, record
          end
        end

        class JSONParser
          include Configurable

          config_param :time_key, :string, :default => 'time'
          config_param :time_format, :string, :default => nil

          def call(text)
            record = Yajl.load(text)

            if value = record.delete(@time_key)
              if @time_format
                time = Time.strptime(value, @time_format).to_i
              else
                time = value.to_i
              end
            end

            return time, record
          rescue Yajl::ParseError
            Fluentd.log.warn "pattern not match: #{text.inspect}: #{$!}"
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
          end

          def values_map(values)
            record = Hash[keys.zip(values)]

            if @time_key
              value = record.delete(@time_key)
              if @time_format
                time = Time.strptime(value, @time_format).to_i
              else
                time = Time.parse(value).to_i
              end
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
            return Time.now.to_i, record
          end
        end

        class ApacheParser
          include Configurable

          REGEXP = /^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/

          def call(text)
            m = REGEXP.match(text)
            unless m
              Fluentd.log.warn "pattern not match: #{text.inspect}"
              return nil, nil
            end

            host = m['host']
            host = (host == '-') ? nil : host

            user = m['user']
            user = (user == '-') ? nil : user

            time = m['time']
            time = Time.strptime(time, "%d/%b/%Y:%H:%M:%S %z").to_i

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

        class SyslogParser < RegexpParser
          REGEXP = /^(?<time>[^ ]*\s*[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[a-zA-Z0-9_\/\.\-]*)(?:\[(?<pid>[0-9]+)\])?[^\:]*\: *(?<message>.*)$/
          TIME_FORMAT = '%b %d %H:%M:%S'

          def initialize
            super(REGEXP)
          end

          def configure(conf)
            super
            @time_format = TIME_FORMAT
          end
        end

        class NginxParser < RegexpParser
          REGEXP = /^(?<remote>[^ ]*) (?<host>[^ ]*) (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^\"]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/
          TIME_FORMAT = '%d/%b/%Y:%H:%M:%S %z'

          def initialize
            super(REGEXP)
          end

          def configure(conf)
            super
            @time_format = TIME_FORMAT
          end
        end

        TEMPLATE_FACTORIES = {
          'apache2' => Proc.new { ApacheParser.new },
          'syslog' => Proc.new { SyslogParser.new },
          'json' => Proc.new { JSONParser.new },
          'tsv' => Proc.new { TSVParser.new },
          'ltsv' => Proc.new { LabeledTSVParser.new },
          'csv' => Proc.new { CSVParser.new },
          'nginx' => Proc.new { NginxParser.new },
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

            @parser = RegexpParser.new(regexp)

          else
            # built-in template
            factory = TEMPLATE_FACTORIES[format]
            unless factory
              raise ConfigError, "Unknown format template '#{format}'"
            end
            @parser = factory.call
          end

          if @parser.respond_to?(:configure)
            @parser.init_configurable
            @parser.configure(conf)
          end

          return true
        end

        def parse(text)
          return @parser.call(text)
        end
      end
    end
  end
end
