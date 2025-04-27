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

    def each(unpacker: nil, &block)
      raise NotImplementedError, "DO NOT USE THIS CLASS directly."
    end

    def to_msgpack_stream(time_int: false, packer: nil)
      return to_msgpack_stream_forced_integer(packer: packer) if time_int
      out = packer || Fluent::MessagePackFactory.msgpack_packer
      each {|time,record|
        out.write([time,record])
      }
      out.full_pack
    end

    def to_compressed_msgpack_stream(time_int: false, packer: nil, type: :gzip)
      packed = to_msgpack_stream(time_int: time_int, packer: packer)
      compress(packed, type: type)
    end

    def to_msgpack_stream_forced_integer(packer: nil)
      out = packer || Fluent::MessagePackFactory.msgpack_packer
      each {|time,record|
        out.write([time.to_i,record])
      }
      out.full_pack
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

    def empty?
      false
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

    def each(unpacker: nil)
      yield(@time, @record)
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

    def each(unpacker: nil, &block)
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

    def each(unpacker: nil)
      time_array = @time_array
      record_array = @record_array
      for i in 0..time_array.length-1
        yield(time_array[i], record_array[i])
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

    def ensure_unpacked!(unpacker: nil)
      return if @unpacked_times && @unpacked_records
      @unpacked_times = []
      @unpacked_records = []
      (unpacker || Fluent::MessagePackFactory.msgpack_unpacker).feed_each(@data) do |time, record|
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

    def each(unpacker: nil)
      ensure_unpacked!(unpacker: unpacker)
      @unpacked_times.each_with_index do |time, i|
        yield(time, @unpacked_records[i])
      end
      nil
    end

    def to_msgpack_stream(time_int: false, packer: nil)
      # time_int is always ignored because @data is always packed binary in this class
      @data
    end
  end

  class CompressedMessagePackEventStream < MessagePackEventStream
    def initialize(data, cached_unpacker = nil, size = 0, unpacked_times: nil, unpacked_records: nil, compress: :gzip)
      super(data, cached_unpacker, size, unpacked_times: unpacked_times, unpacked_records: unpacked_records)
      @decompressed_data = nil
      @compressed_data = data
      @type = compress
    end

    def empty?
      ensure_decompressed!
      super
    end

    def ensure_unpacked!(unpacker: nil)
      ensure_decompressed!
      super
    end

    def each(unpacker: nil, &block)
      ensure_decompressed!
      super
    end

    def to_msgpack_stream(time_int: false, packer: nil)
      ensure_decompressed!
      super
    end

    def to_compressed_msgpack_stream(time_int: false, packer: nil)
      # time_int is always ignored because @data is always packed binary in this class
      @compressed_data
    end

    private

    def ensure_decompressed!
      return if @decompressed_data
      @data = @decompressed_data = decompress(@data, type: @type)
    end
  end

  module ChunkMessagePackEventStreamer
    # chunk.extend(ChunkMessagePackEventStreamer)
    #  => chunk.each{|time, record| ... }
    def each(unpacker: nil, &block)
      # Note: If need to use `unpacker`, then implement it,
      # e.g., `unpacker.feed_each(io.read, &block)` (Not tested)
      raise NotImplementedError, "'unpacker' argument is not implemented." if unpacker

      open do |io|
        Fluent::MessagePackFactory.msgpack_unpacker(io).each(&block)
      end
      nil
    end
    alias :msgpack_each :each

    def to_msgpack_stream(time_int: false, packer: nil)
      # time_int is always ignored because data is already packed and written in chunk
      read
    end
  end
end
