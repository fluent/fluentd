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

require 'fluent/msgpack_factory'
require 'fluent/plugin/compressable'

module Fluent
  class EventStream
    include Enumerable
    include MessagePackFactory::Mixin
    include Fluent::Plugin::Compressable

    # dup does deep copy for event stream
    def dup
      raise NotImplementedError, "DO NOT USE THIS CLASS directly."
    end

    def size
      raise NotImplementedError, "DO NOT USE THIS CLASS directly."
    end
    alias :length :size

    def empty?
      size == 0
    end

    # for tests
    def ==(other)
      other.is_a?(EventStream) && self.to_msgpack_stream == other.to_msgpack_stream
    end

    def repeatable?
      false
    end

    def slice(index, num)
      raise NotImplementedError, "DO NOT USE THIS CLASS directly."
    end

    def each(&block)
      raise NotImplementedError, "DO NOT USE THIS CLASS directly."
    end

    def to_msgpack_stream(time_int: false)
      return to_msgpack_stream_forced_integer if time_int
      out = msgpack_packer
      each {|time,record|
        out.write([time,record])
      }
      out.to_s
    end

    def to_compressed_msgpack_stream(time_int: false)
      packed = to_msgpack_stream(time_int: time_int)
      compress(packed)
    end

    def to_msgpack_stream_forced_integer
      out = msgpack_packer
      each {|time,record|
        out.write([time.to_i,record])
      }
      out.to_s
    end
  end

  class OneEventStream < EventStream
    def initialize(time, record)
      @time = time
      @record = record
    end

    def dup
      OneEventStream.new(@time, @record.dup)
    end

    def size
      1
    end

    def repeatable?
      true
    end

    def slice(index, num)
      if index > 0 || num == 0
        ArrayEventStream.new([])
      else
        self.dup
      end
    end

    def each(&block)
      block.call(@time, @record)
      nil
    end
  end

  # EventStream from entries: Array of [time, record]
  #
  # Use this class for many events data with a tag
  # and its representation is [ [time, record], [time, record], .. ]
  class ArrayEventStream < EventStream
    def initialize(entries)
      @entries = entries
    end

    def dup
      entries = @entries.map{ |time, record| [time, record.dup] }
      ArrayEventStream.new(entries)
    end

    def size
      @entries.size
    end

    def repeatable?
      true
    end

    def empty?
      @entries.empty?
    end

    def slice(index, num)
      ArrayEventStream.new(@entries.slice(index, num))
    end

    def each(&block)
      @entries.each(&block)
      nil
    end
  end

  # EventStream from entries: numbers of pairs of time and record.
  #
  # This class can handle many events more efficiently than ArrayEventStream
  # because this class generate less objects than ArrayEventStream.
  #
  # Use this class as below, in loop of data-enumeration:
  #  1. initialize blank stream:
  #     streams[tag] ||= MultiEventStream.new
  #  2. add events
  #     stream[tag].add(time, record)
  class MultiEventStream < EventStream
    def initialize(time_array = [], record_array = [])
      @time_array = time_array
      @record_array = record_array
    end

    def dup
      MultiEventStream.new(@time_array.dup, @record_array.map(&:dup))
    end

    def size
      @time_array.size
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

    def slice(index, num)
      MultiEventStream.new(@time_array.slice(index, num), @record_array.slice(index, num))
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

  class MessagePackEventStream < EventStream
    # https://github.com/msgpack/msgpack-ruby/issues/119

    # Keep cached_unpacker argument for existing plugins
    def initialize(data, cached_unpacker = nil, size = 0, unpacked_times: nil, unpacked_records: nil)
      @data = data
      @size = size
      @unpacked_times = unpacked_times
      @unpacked_records = unpacked_records
    end

    def empty?
      @data.empty?
    end

    def dup
      if @unpacked_times
        self.class.new(@data.dup, nil, @size, unpacked_times: @unpacked_times, unpacked_records: @unpacked_records.map(&:dup))
      else
        self.class.new(@data.dup, nil, @size)
      end
    end

    def size
      # @size is unbelievable always when @size == 0
      # If the number of events is really zero, unpacking events takes very short time.
      ensure_unpacked! if @size == 0
      @size
    end

    def repeatable?
      true
    end

    def ensure_unpacked!
      return if @unpacked_times && @unpacked_records
      @unpacked_times = []
      @unpacked_records = []
      msgpack_unpacker.feed_each(@data) do |time, record|
        @unpacked_times << time
        @unpacked_records << record
      end
      # @size should be updated always right after unpack.
      # The real size of unpacked objects are correct, rather than given size.
      @size = @unpacked_times.size
    end

    # This method returns MultiEventStream, because there are no reason
    # to surve binary serialized by msgpack.
    def slice(index, num)
      ensure_unpacked!
      MultiEventStream.new(@unpacked_times.slice(index, num), @unpacked_records.slice(index, num))
    end

    def each(&block)
      if @unpacked_times
        @unpacked_times.each_with_index do |time, i|
          block.call(time, @unpacked_records[i])
        end
      else
        @unpacked_times = []
        @unpacked_records = []
        msgpack_unpacker.feed_each(@data) do |time, record|
          @unpacked_times << time
          @unpacked_records << record
          block.call(time, record)
        end
        @size = @unpacked_times.size
      end
      nil
    end

    def to_msgpack_stream(time_int: false)
      # time_int is always ignored because @data is always packed binary in this class
      @data
    end
  end

  class CompressedMessagePackEventStream < MessagePackEventStream
    def initialize(data, cached_unpacker = nil, size = 0, unpacked_times: nil, unpacked_records: nil)
      super
      @decompressed_data = nil
      @compressed_data = data
    end

    def empty?
      ensure_decompressed!
      super
    end

    def ensure_unpacked!
      ensure_decompressed!
      super
    end

    def each(&block)
      ensure_decompressed!
      super
    end

    def to_msgpack_stream(time_int: false)
      ensure_decompressed!
      super
    end

    def to_compressed_msgpack_stream(time_int: false)
      # time_int is always ignored because @data is always packed binary in this class
      @compressed_data
    end

    private

    def ensure_decompressed!
      return if @decompressed_data
      @data = @decompressed_data = decompress(@data)
    end
  end

  module ChunkMessagePackEventStreamer
    include MessagePackFactory::Mixin
    # chunk.extend(ChunkEventStreamer)
    #  => chunk.each{|time, record| ... }
    def each(&block)
      open do |io|
        msgpack_unpacker(io).each(&block)
      end
      nil
    end
    alias :msgpack_each :each

    def to_msgpack_stream(time_int: false)
      # time_int is always ignored because data is already packed and written in chunk
      read
    end
  end
end
