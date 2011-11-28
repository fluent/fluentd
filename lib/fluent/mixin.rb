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


class TimeFormatter
  def initialize(format, localtime)
    @tc1 = 0
    @tc1_str = nil
    @tc2 = 0
    @tc2_str = nil

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

  def self.configure(conf)
    if time_format = conf['time_format']
      @time_format = time_format
    end

    if localtime = conf['localtime']
      @localtime = true
    elsif utc = conf['utc']
      @localtime = false
    end

    @timef = new(@time_format, @localtime)
  end
end


module RecordFilterMixin
  def filter_record(tag, time, record)
  end

  def format_stream(tag, es)
    out = ''
    es.each {|time,record|
      filter_record(tag, time, record)
      out << format(tag, time, record)
    }
    out
  end
end


module SetTimeKeyMixin
  include RecordFilterMixin

  attr_accessor :include_time_key, :time_key, :localtime

  def configure(conf)
    super

    if s = conf['include_time_key']
      b = Config.bool_value(s)
      if s.empty?
        b = true
      elsif b == nil
        raise ConfigError, "Invalid boolean expression '#{s}' for include_time_key parameter"
      end
      @include_time_key = b
    end

    if @include_time_key
      if time_key = conf['time_key']
        @time_key = time_key
      end
      unless @time_key
        @time_key = 'time'
      end

      if time_format = conf['time_format']
        @time_format = time_format
      end

      if localtime = conf['localtime']
        @localtime = true
      elsif utc = conf['utc']
        @localtime = false
      end

      @timef = TimeFormatter.new(@time_format, @localtime)

    else
      @include_time_key = false
    end
  end

  def filter_record(tag, time, record)
    super
    if @include_time_key
      record[@time_key] = @timef.format(time)
    end
  end
end


module SetTagKeyMixin
  include RecordFilterMixin

  attr_accessor :include_tag_key, :tag_key

  def configure(conf)
    super

    if s = conf['include_tag_key']
      b = Config.bool_value(s)
      if s.empty?
        b = true
      elsif b == nil
        raise ConfigError, "Invalid boolean expression '#{s}' for include_tag_key parameter"
      end
      @include_tag_key = b
    end

    if @include_tag_key
      if tag_key = conf['tag_key']
        @tag_key = tag_key
      end
      unless @tag_key
        @tag_key = 'tag'
      end

    else
      @include_tag_key = false
    end
  end

  def filter_record(tag, time, record)
    super
    if @include_tag_key
      record[@tag_key] = tag
    end
  end
end

module PlainTextFormatterMixin
  attr_accessor :output_include_time, :output_include_tag, :output_data_type
  attr_accessor :add_newline, :field_separator

  # config_param :output_data_type, :string, :default => 'json' # or 'attr:field' or 'attr:field1,field2,field3(...)'
  def configure(conf)
    super

    @output_include_time ||= true
    @output_include_tag ||= true
    @output_data_type ||= 'json'

    @field_separator = case @field_separator
                       when 'SPACE' then ' '
                       when 'COMMA' then ','
                       else "\t"
                       end
    @add_newline = Fluent::Config.bool_value(conf['add_newline'])
    if @add_newline.nil?
      @add_newline = true
    end

    # default timezone: utc
    if @localtime.nil? and @utc.nil?
      @utc = true
      @localtime = false
    elsif not @localtime and not @utc
      @utc = true
      @localtime = false
    end
    # mix-in default time formatter (or you can overwrite @timef on your own configure)
    @timef = @output_include_time ? Fluent::TimeFormatter.new(@time_format, @localtime) : nil

    @custom_attributes = []
    if @output_data_type == 'json'
      self.instance_eval {
        def stringify_record(record)
          record.to_json
        end
      }
    elsif @output_data_type =~ /^attr:(.*)$/
      @custom_attributes = $1.split(',')
      if @custom_attributes.size > 1
        self.instance_eval {
          def stringify_record(record)
            @custom_attributes.map{|attr| (record[attr] || 'NULL').to_s}.join(@field_separator)
          end
        }
      elsif @custom_attributes.size == 1
        self.instance_eval {
          def stringify_record(record)
            (record[@custom_attributes[0]] || 'NULL').to_s
          end
        }
      else
        raise Fluent::ConfigError, "Invalid attributes specification: '#{@output_data_type}', needs one or more attributes."
      end
    else
      raise Fluent::ConfigError, "Invalid output_data_type: '#{@output_data_type}'. specify 'json' or 'attr:ATTRIBUTE_NAME' or 'attr:ATTR1,ATTR2,...'"
    end

    if @output_include_time and @output_include_tag
      if @add_newline
        self.instance_eval {
          def format(tag,time,record)
            @timef.format(time) + @field_separator + tag + @field_separator + stringify_record(record) + "\n"
          end
        }
      else
        self.instance_eval {
          def format(tag,time,record)
            @timef.format(time) + @field_separator + tag + @field_separator + stringify_record(record)
          end
        }
      end
    elsif @output_include_time
      if @add_newline
        self.instance_eval {
          def format(tag,time,record);
            @timef.format(time) + @field_separator + stringify_record(record) + "\n"
          end
        }
      else
        self.instance_eval {
          def format(tag,time,record);
            @timef.format(time) + @field_separator + stringify_record(record)
          end
        }
      end
    elsif @output_include_tag
      if @add_newline
        self.instance_eval {
          def format(tag,time,record);
            tag + @field_separator + stringify_record(record) + "\n"
          end
        }
      else
        self.instance_eval {
          def format(tag,time,record);
            tag + @field_separator + stringify_record(record)
          end
        }
      end
    else # without time, tag
      if @add_newline
        self.instance_eval {
          def format(tag,time,record);
            stringify_record(record) + "\n"
          end
        }
      else
        self.instance_eval {
          def format(tag,time,record);
            stringify_record(record)
          end
        }
      end
    end
  end

  def stringify_record(record)
    record.to_json
  end

  def format(tag, time, record)
    time_str = @timef.format(time)
    time_str + @field_separator + tag + @field_separator + stringify_record(record) + "\n"
  end

end

end
