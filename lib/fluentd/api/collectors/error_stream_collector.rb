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
  module Collectors

    class ErrorStreamCollector
      include Collector

      class ErrorStreamWriter
        include Writer

        def initialize(writer, error_collector, tag)
          @writer = writer
          @error_collector = error_collector
          @tag = tag
          @error_writer = nil
        end

        def append(time, record)
          @writer.append(time, record)
        rescue
          @error_writer ||= @error_collector.open(@tag)
          @error_writer.append(time, record)
        end

        def write(chunk)
          @writer.write(chunk)
        rescue
          @error_writer ||= @error_collector.open(@tag)
          @error_writer.write(chunk)
        end

        def close
          @writer.close
          @error_writer.close if @error_writer
        end
      end

      def initialize(collector, error_collector)
        @collector = collector
        @error_collector = error_collector
      end

      def open(tag)
        return ensure_close(open(tag), &proc) if block_given?

        w = @collector.open(tag)
        return ErrorStreamWriter.new(w, @error_collector, tag)
      end
    end

  end
end
