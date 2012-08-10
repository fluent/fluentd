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

  class InputBackwardCompatWrapper < Fluentd::Agent
    class Factory
      def initialize(klass)
        @klass = klass
      end

      def new
        InputBackwardCompatWrapper.new(@klass.new)
      end
    end

    def initialize(base)
      @base = base
    end

    def start
      @base.start
    end

    def stop
      @base.shutdown
    end

    def configure(conf)
      # TODO config wrapper
      @base.configure(conf)
    end

    def shutdown
    end
  end

  class InputForwardCompatWrapper
    include Fluentd::StreamSource

    def initialize(source)
      @source = source
    end

    def configure(conf)
      # TODO config wrapper
      @source.configure(conf)
    end

    def start
      @source.start
    end

    def shutdown
      @source.stop
      @source.shutdown
    end
  end

end
