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
require 'fluent/error'

module Fluent
  module Plugin
    module Extractor
      class SizeLimitError < UnrecoverableError; end

      BYTES_TO_READ = 64 * 1024
      INFLATE_BYTES_TO_READ = 1024

      def self.decompress_gzip(compressed_data, limit:)
        io = StringIO.new(compressed_data)
        out = ''
        loop do
          reader = Zlib::GzipReader.new(io)
          while (chunk = reader.read(BYTES_TO_READ))
            out << chunk
            if out.bytesize > limit
              raise SizeLimitError, "Decompressed data exceeds limit of #{limit} bytes"
            end
          end

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

      def self.decompress_zstd(compressed_data, limit:)
        io = StringIO.new(compressed_data)
        reader = Zstd::StreamReader.new(io)
        out = ''
        loop do
          # Zstd::StreamReader needs to specify the size of the buffer
          out << reader.read(BYTES_TO_READ)
          if out.bytesize > limit
            raise SizeLimitError, "Decompressed data exceeds limit of #{limit} bytes"
          end

          # Zstd::StreamReader doesn't provide unused data, so we have to manually adjust the position
          break if io.eof?
        end
        out
      end

      def self.decompress_deflate(compressed_data, limit:)
        io = StringIO.new(compressed_data)
        out = ''
        begin
          zstream = Zlib::Inflate.new
          while (chunk = io.read(INFLATE_BYTES_TO_READ))
            out << zstream.inflate(chunk)
            if out.bytesize > limit
              raise SizeLimitError, "Decompressed data exceeds limit of #{limit} bytes"
            end
          end
          out << zstream.finish
          if out.bytesize > limit
            raise SizeLimitError, "Decompressed data exceeds limit of #{limit} bytes"
          end
        ensure
          zstream&.close
        end
        out
      end

      def self.io_decompress_gzip(input, output)
        loop do
          reader = Zlib::GzipReader.new(input)
          while (chunk = reader.read(BYTES_TO_READ))
            output.write(chunk)
          end
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

      def self.io_decompress_zstd(input, output)
        reader = Zstd::StreamReader.new(input)
        loop do
          # Zstd::StreamReader needs to specify the size of the buffer
          chunk = reader.read(BYTES_TO_READ)
          output.write(chunk)
          # Zstd::StreamReader doesn't provide unused data, so we have to manually adjust the position
          break if input.eof?
        end
        output
      end
    end
  end
end
