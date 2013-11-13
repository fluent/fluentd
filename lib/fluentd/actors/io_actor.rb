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
module Fluentd
  module Actors

    module IOActor
      def watch_io_readable(io, &callback)
        @loop.attach ReadableIOWatcher.new(io, callback)
      end

      alias_method :watch_io, :watch_io_readable

      def watch_io_writable(io, &callback)
        @loop.attach WritableIOWatcher.new(io, callback)
      end

      class ReadableIOWatcher < Coolio::IOWatcher
        def initialize(io, callback)
          super(io, "r")
          @io = io
          @callback = callback
        end

        def on_readable
          @callback.call(self)
        end
      end

      class WritableIOWatcher < Coolio::IOWatcher
        def initialize(io, callback)
          super(io, "w")
          @io = io
          @callback = callback
        end

        def on_readable
          @callback.call(self)
        rescue => e
          # close io on error
          close
          detach
          Engine.log.error e.to_s
          Engine.log.error_backtrace e.backtrace
        end
      end
    end

  end
end
