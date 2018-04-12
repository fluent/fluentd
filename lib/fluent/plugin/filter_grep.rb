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

    def initialize
      super

      @_regexp_and_conditions = nil
      @_exclude_and_conditions = nil
      @_regexp_or_conditions = nil
      @_exclude_or_conditions = nil
    end

    # for test
    attr_reader :_regexp_and_conditions, :_exclude_and_conditions, :_regexp_or_conditions, :_exclude_or_conditions

    helpers :record_accessor

    REGEXP_MAX_NUM = 20

    (1..REGEXP_MAX_NUM).each {|i| config_param :"regexp#{i}",  :string, default: nil, deprecated: "Use <regexp> section" }
    (1..REGEXP_MAX_NUM).each {|i| config_param :"exclude#{i}", :string, default: nil, deprecated: "Use <exclude> section" }

    config_section :regexp, param_name: :regexps, multi: true do
      desc "The field name to which the regular expression is applied."
      config_param :key, :string
      desc "The regular expression."
      config_param :pattern, :regexp
    end

    config_section :exclude, param_name: :excludes, multi: true do
      desc "The field name to which the regular expression is applied."
      config_param :key, :string
      desc "The regular expression."
      config_param :pattern, :regexp
    end

    config_section :and, param_name: :and_conditions, multi: true do
      config_section :regexp, param_name: :regexps, multi: true do
        desc "The field name to which the regular expression is applied."
        config_param :key, :string
        desc "The regular expression."
        config_param :pattern, :regexp
      end
      config_section :exclude, param_name: :excludes, multi: true do
        desc "The field name to which the regular expression is applied."
        config_param :key, :string
        desc "The regular expression."
        config_param :pattern, :regexp
      end
    end

    config_section :or, param_name: :or_conditions, multi: true do
      config_section :regexp, param_name: :regexps, multi: true do
        desc "The field name to which the regular expression is applied."
        config_param :key, :string
        desc "The regular expression."
        config_param :pattern, :regexp
      end
      config_section :exclude, param_name: :excludes, multi: true do
        desc "The field name to which the regular expression is applied."
        config_param :key, :string
        desc "The regular expression."
        config_param :pattern, :regexp
      end
    end

    def configure(conf)
      super

      regexp_and_conditions = {}
      regexp_or_conditions = {}
      exclude_and_conditions = {}
      exclude_or_conditions = {}

      (1..REGEXP_MAX_NUM).each do |i|
        next unless conf["regexp#{i}"]
        key, regexp = conf["regexp#{i}"].split(/ /, 2)
        raise Fluent::ConfigError, "regexp#{i} does not contain 2 parameters" unless regexp
        raise Fluent::ConfigError, "regexp#{i} contains a duplicated key, #{key}" if regexp_and_conditions[key]
        regexp_and_conditions[key] = Expression.new(record_accessor_create(key), Regexp.compile(regexp))
      end

      (1..REGEXP_MAX_NUM).each do |i|
        next unless conf["exclude#{i}"]
        key, exclude = conf["exclude#{i}"].split(/ /, 2)
        raise Fluent::ConfigError, "exclude#{i} does not contain 2 parameters" unless exclude
        raise Fluent::ConfigError, "exclude#{i} contains a duplicated key, #{key}" if exclude_or_conditions[key]
        exclude_or_conditions[key] = Expression.new(record_accessor_create(key), Regexp.compile(exclude))
      end

      if @regexps.size > 1
        log.info "Top level multiple <regexp> is intepreted as 'and' condition"
      end
      @regexps.each do |e|
        raise Fluent::ConfigError, "Duplicate key: #{e.key}" if regexp_and_conditions.key?(e.key)
        regexp_and_conditions[e.key] = Expression.new(record_accessor_create(e.key), e.pattern)
      end

      if @excludes.size > 1
        log.info "Top level multiple <exclude> is intepreted as 'or' condition"
      end
      @excludes.each do |e|
        raise Fluent::ConfigError, "Duplicate key: #{e.key}" if exclude_or_conditions.key?(e.key)
        exclude_or_conditions[e.key] = Expression.new(record_accessor_create(e.key), e.pattern)
      end

      @and_conditions.each do |and_condition|
        if !and_condition.regexps.empty? && !and_condition.excludes.empty?
          raise Fluent::ConfigError, "Do not specify both <regexp> and <exclude> in <and>"
        end
        and_condition.regexps.each do |e|
          raise Fluent::ConfigError, "Duplicate key in <and>: #{e.key}" if regexp_and_conditions.key?(e.key)
          regexp_and_conditions[e.key] = Expression.new(record_accessor_create(e.key), e.pattern)
        end
        and_condition.excludes.each do |e|
          raise Fluent::ConfigError, "Duplicate key in <and>: #{e.key}" if exclude_and_conditions.key?(e.key)
          exclude_and_conditions[e.key] = Expression.new(record_accessor_create(e.key), e.pattern)
        end
      end

      @or_conditions.each do |or_condition|
        if !or_condition.regexps.empty? && !or_condition.excludes.empty?
          raise Fluent::ConfigError, "Do not specify both <regexp> and <exclude> in <or>"
        end
        or_condition.regexps.each do |e|
          raise Fluent::ConfigError, "Duplicate key in <or>: #{e.key}" if regexp_or_conditions.key?(e.key)
          regexp_or_conditions[e.key] = Expression.new(record_accessor_create(e.key), e.pattern)
        end
        or_condition.excludes.each do |e|
          raise Fluent::ConfigError, "Duplicate key in <or>: #{e.key}" if exclude_or_conditions.key?(e.key)
          exclude_or_conditions[e.key] = Expression.new(record_accessor_create(e.key), e.pattern)
        end
      end

      @_regexp_and_conditions = regexp_and_conditions.values unless regexp_and_conditions.empty?
      @_exclude_and_conditions = exclude_and_conditions.values unless exclude_and_conditions.empty?
      @_regexp_or_conditions = regexp_or_conditions.values unless regexp_or_conditions.empty?
      @_exclude_or_conditions = exclude_or_conditions.values unless exclude_or_conditions.empty?
    end

    def filter(tag, time, record)
      begin
        if @_regexp_and_conditions && @_regexp_and_conditions.any? { |expression| !expression.match?(record) }
          return nil
        end
        if @_regexp_or_conditions && @_regexp_or_conditions.none? { |expression| expression.match?(record) }
          return nil
        end
        if @_exclude_and_conditions && @_exclude_and_conditions.all? { |expression| expression.match?(record) }
          return nil
        end
        if @_exclude_or_conditions && @_exclude_or_conditions.any? { |expression| expression.match?(record) }
          return nil
        end
      rescue => e
        log.warn "failed to grep events", error: e
        log.warn_backtrace
      end
      record
    end

    Expression = Struct.new(:key, :pattern) do
      def match?(record)
        ::Fluent::StringUtil.match_regexp(pattern, key.call(record).to_s)
      end
    end
  end
end
