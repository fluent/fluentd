#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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
module Fluentd
  module Plugin

    require 'fileutils'

    class BufferError < StandardError
    end

    class BufferRecordLimitError < BufferError
    end

    class BufferChunkLimitError < BufferError
    end

    class BufferQueueLimitError < BufferError
    end

    class Buffer
      include Configurable

      def open(&block)
        raise NoMethodError, "#{self.class}#synchronize(&block) is not implemented"
      end

      def acquire(&block)
        raise NoMethodError, "#{self.class}#acquire(&block) is not implemented"
      end

      def clear
        raise NoMethodError, "#{self.class}#clear is not implemented"
      end

      def start
      end

      def stop
      end

      def shutdown
      end
    end

    class BufferChunk
      def initialize(key)
        @key = key
      end

      attr_reader :key

      def size
        raise NoMethodError, "#{self.class}#size is not implemented"
      end

      def empty?
        size == 0
      end

      def open(&block)
        raise NoMethodError, "#{self.class}#open(&block) is not implemented"
      end

      def read
        raise NoMethodError, "#{self.class}#read is not implemented"
      end

      def write_to(io)
        open {|i|
          FileUtils.copy_stream(i, io)
        }
      end

      def msgpack_each(&block)
        open {|io|
          u = MessagePack::Unpacker.new(io)
          u.each(&block)
        }
      end
    end

  end
end
