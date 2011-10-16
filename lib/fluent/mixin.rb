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

  attr_accessor :time_key, :localtime

  def configure(conf)
    super

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
  end

  def filter_record(tag, time, record)
    super
    record[@time_key] = @timef.format(time)
  end
end


module SetTagKeyMixin
  include RecordFilterMixin

  attr_accessor :tag_key

  def configure(conf)
    super

    if tag_key = conf['tag_key']
      @tag_key = tag_key
    end
    unless @tag_key
      @tag_key = 'tag'
    end
  end

  def filter_record(tag, time, record)
    super
    record[@tag_key] = tag
  end
end


end
