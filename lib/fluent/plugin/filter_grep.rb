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

require 'fluent/plugin/filter'
require 'fluent/config/error'
require 'fluent/plugin/string_util'

module Fluent::Plugin
  class GrepFilter < Filter
    Fluent::Plugin.register_filter('grep', self)

    REGEXP_MAX_NUM = 20

    (1..REGEXP_MAX_NUM).each {|i| config_param :"regexp#{i}",  :string, default: nil, deprecated: "Use <regexp> section" }
    (1..REGEXP_MAX_NUM).each {|i| config_param :"exclude#{i}", :string, default: nil, deprecated: "Use <exclude> section" }

    config_section :regexp, param_name: :regexps, multi: true do
      desc "The field name to which the regular expression is applied."
      config_param :key, :string
      desc "The regular expression."
      config_param :pattern do |value|
        Regexp.compile(value)
      end
    end

    config_section :exclude, param_name: :excludes, multi: true do
      desc "The field name to which the regular expression is applied."
      config_param :key, :string
      desc "The regular expression."
      config_param :pattern do |value|
        Regexp.compile(value)
      end
    end

    # for test
    attr_reader :_regexps
    attr_reader :_excludes

    def configure(conf)
      super

      @_regexps = {}
      (1..REGEXP_MAX_NUM).each do |i|
        next unless conf["regexp#{i}"]
        key, regexp = conf["regexp#{i}"].split(/ /, 2)
        raise Fluent::ConfigError, "regexp#{i} does not contain 2 parameters" unless regexp
        raise Fluent::ConfigError, "regexp#{i} contains a duplicated key, #{key}" if @_regexps[key]
        @_regexps[key] = Regexp.compile(regexp)
      end

      @_excludes = {}
      (1..REGEXP_MAX_NUM).each do |i|
        next unless conf["exclude#{i}"]
        key, exclude = conf["exclude#{i}"].split(/ /, 2)
        raise Fluent::ConfigError, "exclude#{i} does not contain 2 parameters" unless exclude
        raise Fluent::ConfigError, "exclude#{i} contains a duplicated key, #{key}" if @_excludes[key]
        @_excludes[key] = Regexp.compile(exclude)
      end

      @regexps.each do |e|
        raise Fluent::ConfigError, "Duplicate key: #{e.key}" if @_regexps.key?(e.key)
        @_regexps[e.key] = e.pattern
      end
      @excludes.each do |e|
        raise Fluent::ConfigError, "Duplicate key: #{e.key}" if @_excludes.key?(e.key)
        @_excludes[e.key] = e.pattern
      end
    end

    def filter(tag, time, record)
      result = nil
      begin
        catch(:break_loop) do
          @_regexps.each do |key, regexp|
            throw :break_loop unless ::Fluent::StringUtil.match_regexp(regexp, record[key].to_s)
          end
          @_excludes.each do |key, exclude|
            throw :break_loop if ::Fluent::StringUtil.match_regexp(exclude, record[key].to_s)
          end
          result = record
        end
      rescue => e
        log.warn "failed to grep events", error: e
        log.warn_backtrace
      end
      result
    end
  end
end
