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
module Fluent

  class OutputBackwardCompatWrapper
    class Factory
      def initialize(klass)
        @klass = klass
      end

      def new
        OutputBackwardCompatWrapper.new(@klass.new)
      end
    end

    class OutputBackwardCompatWriter
      include Fluentd::Collector::Writer

      def initialize(base)
        @base = base
      end

      def append(tag, time, record)
        @base.emit(tag, OneEventStream.new(time, record), NullOutputChain.instance)
      end

      def write(tag, chunk)
        # TODO wrapper
        @base.emit(chunk)
      end
    end

    def initialize(base)
      @base = base
    end

    def open
      OutputBackwardCompatWriter.new(@base)
    end

    def start
      @base.start
    end

    def stop
    end

    def configure(conf)
      # TODO wrapper
      @base.configure(conf)
    end

    def shutdown
      @base.shutdown
    end
  end

  class OutputForwardCompatWrapper < Fluentd::Agent
    include Fluentd::StreamSource

    def initialize(collector)
      @collector = collector
    end

    def emit(tag, es, chain)
      w = @collector.open
      begin
        w.write(tag, es)
      ensure
        w.close
      end
      chain.next
    end
  end

end
