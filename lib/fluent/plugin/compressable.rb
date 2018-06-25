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

module Fluent
  module Plugin
    module Compressable
      def compress(data, **kwargs)
        output_io = kwargs[:output_io]
        io = output_io || StringIO.new
        Zlib::GzipWriter.wrap(io) do |gz|
          gz.write data
        end

        output_io || io.string
      end

      # compressed_data is String like `compress(data1) + compress(data2) + ... + compress(dataN)`
      # https://www.ruby-forum.com/topic/971591#979503
      def decompress(compressed_data = nil, output_io: nil, input_io: nil)
        case
        when input_io && output_io
          io_decompress(input_io, output_io)
        when input_io
          output_io = StringIO.new
          io = io_decompress(input_io, output_io)
          io.string
        when compressed_data.nil? || compressed_data.empty?
          # check compressed_data(String) is 0 length
          compressed_data
        when output_io
          # exeucte after checking compressed_data is empty or not
          io = StringIO.new(compressed_data)
          io_decompress(io, output_io)
        else
          string_decompress(compressed_data)
        end
      end

      private

      def string_decompress(compressed_data)
        io = StringIO.new(compressed_data)

        out = ''
        loop do
          gz = Zlib::GzipReader.new(io)
          out << gz.read
          unused = gz.unused
          gz.finish

          break if unused.nil?
          adjust = unused.length
          io.pos -= adjust
        end

        out
      end

      def io_decompress(input, output)
        loop do
          gz = Zlib::GzipReader.new(input)
          v = gz.read
          output.write(v)
          unused = gz.unused
          gz.finish

          break if unused.nil?
          adjust = unused.length
          input.pos -= adjust
        end

        output
      end
    end
  end
end
