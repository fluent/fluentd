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


#module PlainTextFormatterMixin
#  attr_accessor :output_include_time, :output_include_tag, :output_data_type
#  attr_accessor :output_add_newline, :output_field_separator
#
#  def initialize
#    super
#    # default values may be overwriten by subclasses
#    @output_include_time = true
#    @output_include_tag = true
#    @output_data_type = 'json'
#    @output_field_separator = "\t"
#    @output_add_newline = true
#  end
#
#  # config_param :output_data_type, :string, :default => 'json' # or 'attr:field' or 'attr:field1,field2,field3(...)'
#  def configure(conf)
#    super
#
#    if output_include_time = conf['output_include_time']
#      @output_include_time = Config.bool_value(output_include_time)
#    end
#
#    if output_include_tag = conf['output_include_tag']
#      @output_include_tag = Config.bool_value(output_include_tag)
#    end
#
#    if output_data_type = conf['output_data_type']
#      @output_data_type = output_data_type
#    end
#
#    if output_field_separator = conf['output_field_separator']
#      case output_field_separator
#      when 'SPACE'
#        @output_field_separator = ' '
#      when 'COMMA'
#        @output_field_separator = ','
#      else
#        raise ConfigError, "Unknown output_field_separator option #{output_field_separator.dump}"
#      end
#    end
#
#    if output_add_newline = conf['output_add_newline']
#      @output_add_newline = Config.bool_value(output_add_newline)
#    end
#
#    # default timezone: utc (localtime=nil)
#    if conf['localtime']
#      @localtime = true
#    elsif conf['utc']
#      @localtime = false
#    end
#
#    # mix-in default time formatter (or you can overwrite @time_format/@localtime (or @timef itself) on your own configure)
#    if @output_include_time
#      timef = @timef || TimeFormatter.new(@time_format, @localtime)
#    end
#
#    output_field_separator = @output_field_separator
#
#    ##
#    # optimize stringify_record(record) method
#    #
#    case @output_data_type
#    when 'json'
#      define_singleton_method(:stringify_record) {|record|
#        record.to_json
#      }
#
#    when /^attr:(.*)$/
#      out_keys = $1.split(',')
#      if out_keys.size > 1
#        define_singleton_method(:stringify_record) {|record|
#          out_keys.map {|attr|
#            r = record[attr]
#            r.respond_to?(:to_str) ? r.to_str : r.to_json
#          }.join(output_field_separator)
#        }
#      elsif out_keys.size == 1
#        out_key = out_keys[0]
#        define_singleton_method(:stringify_record) {|record|
#          r = record[out_key]
#          r.respond_to?(:to_str) ? r.to_str : r.to_json
#        }
#      else
#        raise ConfigError, "Invalid attributes specification: '#{@output_data_type}', needs one or more attributes."
#      end
#
#    else
#      raise ConfigError, "Invalid output_data_type: '#{@output_data_type}'. specify 'json' or 'attr:ATTRIBUTE_NAME' or 'attr:ATTR1,ATTR2,...'"
#    end
#
#    ##
#    # optimize format(tag, time, record) method
#    #
#    if @output_include_time and @output_include_tag
#      if @output_add_newline
#        define_singleton_method(:format) {|tag,time,record|
#          "#{timef.format(time)}#{output_field_separator}#{tag}#{output_field_separator}#{stringify_record(record)}\n"
#        }
#      else
#        define_singleton_method(:format) {|tag,time,record|
#          "#{timef.format(time)}#{output_field_separator}#{tag}#{output_field_separator}#{stringify_record(record)}"
#        }
#      end
#
#    elsif @output_include_time
#      if @output_add_newline
#        define_singleton_method(:format) {|tag,time,record|
#          "#{timef.format(time)}#{output_field_separator}#{stringify_record(record)}\n"
#        }
#      else
#        define_singleton_method(:format) {|tag,time,record|
#          "#{timef.format(time)}#{output_field_separator}#{stringify_record(record)}"
#        }
#      end
#
#    elsif @output_include_tag
#      if @output_add_newline
#        define_singleton_method(:format) {|tag,time,record|
#          "#{tag}#{output_field_separator}#{stringify_record(record)}\n"
#        }
#      else
#        define_singleton_method(:format) {|tag,time,record|
#          "#{tag}#{output_field_separator}#{stringify_record(record)}"
#        }
#      end
#
#    else # without time, tag
#      if @output_add_newline
#        define_singleton_method(:format) {|tag,time,record|
#          "#{stringify_record(record)}\n"
#        }
#      else
#        define_singleton_method(:format) {|tag,time,record|
#          stringify_record(record)
#        }
#      end
#    end
#  end
#
#  def stringify_record(record)
#    # will be overridden in configure
#  end
#
#  def format(tag, time, record)
#    # will be overridden in configure
#  end
#end


end
