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
  TEMPLATES = {
    'apache' => [/^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/, "%d/%b/%Y:%H:%M:%S %z"],
    'syslog' => [/^(?<time>[^ ]*\s*[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[a-zA-Z0-9_\/\.\-]*)(?:\[(?<pid>[0-9]+)\])?[^\:]*\: *(?<message>.*)$/, "%b %d %H:%M:%S"],
  }

  def self.register_template(name, regexp, time_format=nil)
    TEMPLATES[name] = [regexp, time_format]
  end

  def self.get_template(name)
    return *TEMPLATES[name]
  end

  def initialize
    require 'time'  # Time.strptime, Time.parse
    @regexp = nil
    @time_format = nil
  end

  attr_accessor :regexp, :time_format

  def use_template(name)
    @regexp, @time_format = TextParser.get_template(name)
    unless @regexp
      raise ConfigError, "Unknown format template '#{name}'"
    end
  end

  def configure(conf, required=true)
    if format = conf['format']
      if format[0] == ?/ && format[format.length-1] == ?/
        # regexp
        begin
          @regexp = Regexp.new(format[1..-2])
          if @regexp.named_captures.empty?
            raise "No named captures"
          end
        rescue
          raise ConfigError, "Invalid regexp '#{format[1..-2]}': #{$!}"
        end

      else
        # template
        use_template(format)
      end
    else
      return nil if !required
      raise ConfigError, "'format' parameter is required"
    end

    if time_format = conf['time_format']
      unless @regexp.names.include?('time')
        raise ConfigError, "'time_format' parameter is invalid when format doesn't have 'time' capture"
      end
      @time_format = time_format
    end

    return true
  end

  def parse(text)
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

    return time, record
  end
end


end
