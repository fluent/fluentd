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

require 'stringio'
require 'zlib'
require 'zstd-ruby'

module Fluent
  module Plugin
    module Compressable
      def compress(data, type: :gzip, **kwargs)
        output_io = kwargs[:output_io]
        io = output_io || StringIO.new
        if type == :gzip
          writer = Zlib::GzipWriter.new(io)
          writer.write(data)
          writer.finish
          io.string
        elsif type == :zstd
          sc = Zstd::StreamingCompress.new(**kwargs)
          chunk_size = kwargs[:chunk_size] || (256 * 1024)

          if data.is_a?(String)
            i = 0
            while i < data.bytesize
              io << sc.compress(data.byteslice(i, chunk_size))
              i += chunk_size
            end
          else
            data.each { |d| io << sc.compress(d) }
          end

          # Need to close frame to prevent unknown frame descriptor errors
          io << sc.finish
          io.string
        else
          raise ArgumentError, "Unknown compression type: #{type}"
        end
      end

      # compressed_data is String like `compress(data1) + compress(data2) + ... + compress(dataN)`
      # https://www.ruby-forum.com/topic/971591#979503
      def decompress(compressed_data = nil, output_io: nil, input_io: nil, type: :gzip)
        case
        when input_io && output_io
          io_decompress(input_io, output_io, type)
        when input_io
          output_io = StringIO.new
          io = io_decompress(input_io, output_io, type)
          io.string
        when compressed_data.nil? || compressed_data.empty?
          # check compressed_data(String) is 0 length
          compressed_data
        when output_io
          # execute after checking compressed_data is empty or not
          io = StringIO.new(compressed_data)
          io_decompress(io, output_io, type)
        else
          string_decompress(compressed_data, type)
        end
      end

      private

      def string_decompress_gzip(compressed_data)
        io = StringIO.new(compressed_data)
        out = ''
        loop do
          reader = Zlib::GzipReader.new(io)
          out << reader.read
          unused = reader.unused
          reader.finish
          unless unused.nil?
            adjust = unused.length
            io.pos -= adjust
          end
          break if io.eof?
        end
        out
      end

      def string_decompress_zstd(compressed_data)
        io = StringIO.new(compressed_data)
        reader = Zstd::StreamReader.new(io)
        out = ''
        loop do
          # Zstd::StreamReader needs to specify the size of the buffer
          out << reader.read(1024)
          # Zstd::StreamReader doesn't provide unused data, so we have to manually adjust the position
          break if io.eof?
        end
        out
      end

      def string_decompress(compressed_data, type = :gzip)
        if type == :gzip
          string_decompress_gzip(compressed_data)
        elsif type == :zstd
          string_decompress_zstd(compressed_data)
        else
          raise ArgumentError, "Unknown compression type: #{type}"
        end
      end

      def io_decompress_gzip(input, output)
        loop do
          reader = Zlib::GzipReader.new(input)
          v = reader.read
          output.write(v)
          unused = reader.unused
          reader.finish
          unless unused.nil?
            adjust = unused.length
            input.pos -= adjust
          end
          break if input.eof?
        end
        output
      end

      def io_decompress_zstd(input, output)
        reader = Zstd::StreamReader.new(input)
        loop do
          # Zstd::StreamReader needs to specify the size of the buffer
          v = reader.read(1024)
          output.write(v)
          # Zstd::StreamReader doesn't provide unused data, so we have to manually adjust the position
          break if input.eof?
        end
        output
      end

      def io_decompress(input, output, type = :gzip)
        if type == :gzip
          io_decompress_gzip(input, output)
        elsif type == :zstd
          io_decompress_zstd(input, output)
        else
          raise ArgumentError, "Unknown compression type: #{type}"
        end
      end
    end
  end
end
