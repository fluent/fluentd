#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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
  module Plugins

    class FileBufferChunk < Buffers::BasicChunk
      def initialize(path, unique_id)
        @path = path
        @unique_id = unique_id
      end

      attr_reader :unique_id

      def size
        @size ||= File.stat(@path).size
      end

      def each(&block)
        File.open(@path) {|f|
          begin
            MessagePack::Unpacker.new(f).each(&block)
          rescue EOFError
          end
        }
      end

      def read
        File.read(@path)
      end
    end

    class FileBuffer < Buffers::BasicBuffer
      def initialize
        require 'uri'
        super
      end

      config_param :buffer_path, :string

      def configure(conf)
        super

        @buffer_path_prefix = @buffer_path+"."
        @buffer_path_suffix = ".log"
        @buffer_path_glob = "#{@buffer_path_prefix}*#{@buffer_path_suffix}"
      end

      def start
        FileUtils.mkdir_p File.dirname(@buffer_path_prefix+"path")
        super
      end

      def open(tag)
        # TODO not implemented yet
      end

      def acquire(&block)
        list_queued_files {|path,timestamp,unique_id|
          c = FileBufferChunk.new(path, unique_id)
          # TODO lock this file
          block.call(c)
          File.unlink(path) rescue nil
          break
        }
      end

      private

      # TODO not implemented yet
      #PATH_BUFFER_MATCH = /^(.*)[\._]b([0-9a-fA-F]{1,32})$/
      #PATH_QUEUED_MATCH = /^(.*)[\._]q([0-9a-fA-F]{1,32})$/

      def list_buffer_files(&block)
        list_files(PATH_BUFFER_MATCH, &block)
      end

      def list_queued_files(&block)
        list_files(PATH_QUEUED_MATCH, &block)
      end

      def list_files(regexp, &block)
        # TODO not implemented yet
      end

      def encode_key(key)
        URI.escape(key, /[^-_.a-zA-Z0-9]/n)
      end

      def decode_key(encoded_key)
        URI.unescape(encoded_key)
      end

      def encode_unique_id(unique_id)
        # TODO not implemented yet
      end

      def decode_unique_id(encoded_unique_id)
        # TODO not implemented yet
      end
    end

  end
end

