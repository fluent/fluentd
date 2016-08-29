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

require 'zlib'

module Fluent
  module Plugin
    module Compressable
      def compress(data)
        io = StringIO.new
        Zlib::GzipWriter.wrap(io) do |gz|
          gz.write data
        end
        io.string
      end

      # compressed_data is String like `compress(data1) + compress(data2) + ... + compress(dataN)`
      # https://www.ruby-forum.com/topic/971591#979503
      def decompress(compressed_data, **kargs)
        io = kargs[:io] || StringIO.new(compressed_data)
        ret = ''

        loop do
          gz = Zlib::GzipReader.new(io)
          ret += gz.read
          unused = gz.unused
          gz.finish

          break if unused.nil?
          adjust = unused.length
          io.pos -= adjust
        end

        ret
      end
    end
  end
end
