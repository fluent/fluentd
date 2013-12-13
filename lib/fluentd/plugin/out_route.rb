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
  module Plugin

    require 'fluentd/plugin/filtering_output'

    class RouteOutput < FilteringOutput
      Plugin.register_output('route', self)

      def configure(conf)
        c = conf.dup
        c.elements = []

        conf.elements.select {|e|
          e.name == 'match'
        }.each {|e|
          # set default type: redirect
          c.elements << e.merge({'type' => 'redirect'})
        }

        super(c)
      end

      def emit(tag, time, record)
        match(tag).emit(tag, time, record)
      end

      def emits(tag, es)
        match(tag).emits(tag, es)
      end

      def match(tag)
        event_router.match(tag)
      end
    end

  end
end
