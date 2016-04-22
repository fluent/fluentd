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
require 'fluent/plugin/buffer'
require 'fluent/unique_id'

require 'fileutils'
require 'monitor'

module Fluent
  module Plugin
    class Buffer # fluent/plugin/buffer is alread loaded
      class Chunk
        include MonitorMixin
        include MessagePackFactory::Mixin
        include UniqueId::Mixin

        # Chunks has 2 part:
        # * metadata: contains metadata which should be restored after resume (if possible)
        #             v: {key=>value,key=>value,...} (optional)
        #             t: tag as string (optional)
        #             k: time slice key (optional)
        #
        #             id: unique_id of chunk (*)
        #             r: number of records (*)
        #             c: created_at as unix time (*)
        #             m: modified_at as unix time (*)
        #              (*): fields automatically injected by chunk itself
        # * data: binary data, combined records represented as String, maybe compressed

        # NOTE: keys of metadata are named with a single letter
        #       to decread bytesize of metadata I/O

        # TODO: CompressedPackedMessage of forward protocol?

        def initialize(metadata)
          super()
          @unique_id = generate_unique_id
          @metadata = metadata

          @records = 0
          @created_at = Time.now
          @modified_at = Time.now
        end

        attr_reader :unique_id, :metadata, :created_at, :modified_at

        # data is array of formatted record string
        def append(data)
          raise NotImplementedError, "Implement this method in child class"
        end

        # for event streams which is packed or zipped (and we want not to unpack/uncompress)
        def concat(bulk, records)
          raise NotImplementedError, "Implement this method in child class"
        end

        def commit
          raise NotImplementedError, "Implement this method in child class"
        end

        def rollback
          raise NotImplementedError, "Implement this method in child class"
        end

        def size
          raise NotImplementedError, "Implement this method in child class"
        end

        def records
          raise NotImplementedError, "Implement this method in child class"
        end

        def empty?
          size == 0
        end

        ## method for post-process of enqueue (e.g., renaming file for file chunks)
        # def enqueued!

        def close
          raise NotImplementedError, "Implement this method in child class"
        end

        def purge
          raise NotImplementedError, "Implement this method in child class"
        end

        def read
          raise NotImplementedError, "Implement this method in child class"
        end

        def open(&block)
          raise NotImplementedError, "Implement this method in child class"
        end

        def write_to(io)
          open do |i|
            FileUtils.copy_stream(i, io)
          end
        end

        def msgpack_each(&block)
          open do |io|
            u = msgpack_factory.unpacker(io)
            begin
              u.each(&block)
            rescue EOFError
            end
          end
        end
      end
    end
  end
end
