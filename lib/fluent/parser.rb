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
  class RegexpParser
    include Configurable

    config_param :time_format, :string, :default => nil

    def initialize(regexp, conf={})
      super()
      @regexp = regexp
      unless conf.empty?
        configure(conf)
      end
    end

    def call(text)
      m = @regexp.match(text)
      unless m
        $log.debug "pattern not match: #{text}"
        # TODO?
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

      time ||= Engine.now

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
      else
        time = Engine.now
      end

      return time, record
    rescue Yajl::ParseError
      # TODO?
      return nil, nil
    end
  end

  TEMPLATES = {
    'apache' => RegexpParser.new(/^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/, {'time_format'=>"%d/%b/%Y:%H:%M:%S %z"}),
    'syslog' => RegexpParser.new(/^(?<time>[^ ]*\s*[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[a-zA-Z0-9_\/\.\-]*)(?:\[(?<pid>[0-9]+)\])?[^\:]*\: *(?<message>.*)$/, {'time_format'=>"%b %d %H:%M:%S"}),
    'json' => JSONParser.new,
  }

  def self.register_template(name, regexp_or_proc, time_format=nil)
    if regexp_or_proc.is_a?(Regexp)
      regexp = regexp_or_proc
      pr = RegexpParser.new(regexp, {'time_format'=>time_format})
    else
      pr = regexp_or_proc
    end

    TEMPLATES[name] = pr
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
      @parser = TEMPLATES[format]
      unless @parser
        raise ConfigError, "Unknown format template '#{format}'"
      end
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
