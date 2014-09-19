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

module Fluent
  class TimeFormatter
    require 'fluent/timezone'

    def initialize(format, localtime, timezone = nil)
      @tc1 = 0
      @tc1_str = nil
      @tc2 = 0
      @tc2_str = nil

      if formatter = Fluent::Timezone.formatter(timezone, format)
        define_singleton_method(:format_nocache) {|time|
          formatter.call(time)
        }
        return
      end

      if format
        if localtime
          define_singleton_method(:format_nocache) {|time|
            Time.at(time).strftime(format)
          }
        else
          define_singleton_method(:format_nocache) {|time|
            Time.at(time).utc.strftime(format)
          }
        end
      else
        if localtime
          define_singleton_method(:format_nocache) {|time|
            Time.at(time).iso8601
          }
        else
          define_singleton_method(:format_nocache) {|time|
            Time.at(time).utc.iso8601
          }
        end
      end
    end

    def format(time)
      if @tc1 == time
        return @tc1_str
      elsif @tc2 == time
        return @tc2_str
      else
        str = format_nocache(time)
        if @tc1 < @tc2
          @tc1 = time
          @tc1_str = str
        else
          @tc2 = time
          @tc2_str = str
        end
        return str
      end
    end

    def format_nocache(time)
      # will be overridden in initialize
    end
  end


  module RecordFilterMixin
    def filter_record(tag, time, record)
    end

    def format_stream(tag, es)
      out = ''
      es.each {|time,record|
        tag_temp = tag.dup
        filter_record(tag_temp, time, record)
        out << format(tag_temp, time, record)
      }
      out
    end
  end

  module HandleTagNameMixin
    include RecordFilterMixin

    attr_accessor :remove_tag_prefix, :remove_tag_suffix, :add_tag_prefix, :add_tag_suffix
    def configure(conf)
      super
      if remove_tag_prefix = conf['remove_tag_prefix']
        @remove_tag_prefix = Regexp.new('^' + Regexp.escape(remove_tag_prefix))
      end

      if remove_tag_suffix = conf['remove_tag_suffix']
        @remove_tag_suffix = Regexp.new(Regexp.escape(remove_tag_suffix) + '$')
      end

      @add_tag_prefix = conf['add_tag_prefix']
      @add_tag_suffix = conf['add_tag_suffix']
    end

    def filter_record(tag, time, record)
      tag.sub!(@remove_tag_prefix, '') if @remove_tag_prefix
      tag.sub!(@remove_tag_suffix, '') if @remove_tag_suffix
      tag.insert(0, @add_tag_prefix) if @add_tag_prefix
      tag << @add_tag_suffix if @add_tag_suffix
      super(tag, time, record)
    end
  end

  module SetTimeKeyMixin
    require 'fluent/timezone'
    include RecordFilterMixin

    attr_accessor :include_time_key, :time_key, :localtime, :timezone

    def configure(conf)
      @include_time_key = false

      super

      if s = conf['include_time_key']
        include_time_key = Config.bool_value(s)
        raise ConfigError, "Invalid boolean expression '#{s}' for include_time_key parameter" if include_time_key.nil?

        @include_time_key = include_time_key
      end

      if @include_time_key
        @time_key     = conf['time_key'] || 'time'
        @time_format  = conf['time_format']

        if    conf['localtime']
          @localtime = true
        elsif conf['utc']
          @localtime = false
        end

        if conf['timezone']
          @timezone = conf['timezone']
          Fluent::Timezone.validate!(@timezone)
        end

        @timef = TimeFormatter.new(@time_format, @localtime, @timezone)
      end
    end

    def filter_record(tag, time, record)
      super

      record[@time_key] = @timef.format(time) if @include_time_key
    end
  end

  module SetTagKeyMixin
    include RecordFilterMixin

    attr_accessor :include_tag_key, :tag_key

    def configure(conf)
      @include_tag_key = false

      super

      if s = conf['include_tag_key']
        include_tag_key = Config.bool_value(s)
        raise ConfigError, "Invalid boolean expression '#{s}' for include_tag_key parameter" if include_tag_key.nil?

        @include_tag_key = include_tag_key
      end

      @tag_key = conf['tag_key'] || 'tag' if @include_tag_key
    end

    def filter_record(tag, time, record)
      super

      record[@tag_key] = tag if @include_tag_key
    end
  end
end
