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


class EventStream
  include Enumerable

  def repeatable?
    false
  end

  #def each(&block)
  #end

  def to_msgpack_stream
    out = ''
    each {|time,record|
      [time,record].to_msgpack(out)
    }
    out
  end
end


class OneEventStream < EventStream
  def initialize(time, record)
    @time = time
    @record = record
  end

  def repeatable?
    true
  end

  def each(&block)
    block.call(@time, @record)
    nil
  end
end


class ArrayEventStream < EventStream
  def initialize(entries)
    @entries = entries
  end

  def repeatable?
    true
  end

  def empty?
    @entries.empty?
  end

  def each(&block)
    @entries.each(&block)
    nil
  end

  #attr_reader :entries
  #
  #def to_a
  #  @entries
  #end
end


class MultiEventStream < EventStream
  def initialize
    @time_array = []
    @record_array = []
  end

  def add(time, record)
    @time_array << time
    @record_array << record
  end

  def repeatable?
    true
  end

  def empty?
    @time_array.empty?
  end

  def each(&block)
    time_array = @time_array
    record_array = @record_array
    for i in 0..time_array.length-1
      block.call(time_array[i], record_array[i])
    end
    nil
  end
end

if $use_msgpack_5

  class MessagePackEventStream < EventStream
    def initialize(data, cached_unpacker=nil)
      @data = data
    end

    def repeatable?
      true
    end

    def each(&block)
      # TODO format check
      unpacker = MessagePack::Unpacker.new
      unpacker.feed_each(@data, &block)
      nil
    end

    def to_msgpack_stream
      @data
    end
  end

else # for 0.4.x. Will be removed after 0.5.x is stable

  class MessagePackEventStream < EventStream
    def initialize(data, cached_unpacker=nil)
      @data = data
      @unpacker = cached_unpacker || MessagePack::Unpacker.new
    end

    def repeatable?
      true
    end

    def each(&block)
      @unpacker.reset
      # TODO format check
      @unpacker.feed_each(@data, &block)
      nil
    end

    def to_msgpack_stream
      @data
    end
  end

end

#class IoEventStream < EventStream
#  def initialize(io, num)
#    @io = io
#    @num = num
#    @u = MessagePack::Unpacker.new(@io)
#  end
#
#  def repeatable?
#    false
#  end
#
#  def each(&block)
#    return nil if @num == 0
#    @u.each {|obj|
#      block.call(obj[0], obj[1])
#      break if @array.size >= @num
#    }
#  end
#end


end

