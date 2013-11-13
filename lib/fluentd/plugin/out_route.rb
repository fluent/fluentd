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
  module Plugin

    require 'fluentd/plugin/routing_output'
    require 'fluentd/collectors/no_match_notice_collector'

    class RouteOutput < RoutingOutput
      Plugin.register_output('route', self)

      def configure(conf)
        c = conf.dup
        c.elements = []

        conf.elements.select {|e|
          e.name == 'match' || e.name == 'copy' || e.name == 'filter'
        }.each {|e|
          c.elements << e.merge({'type' => 'redirect'})
        }

        super(c)

        # TODO this should be moved to Agent if <filter> is removed
        self.default_collector = Collectors::NoMatchNoticeCollector.new(logger)
        #self.default_collector = Collectors::NullCollector.new
      end

      def match(tag)
        #p @event_router
        #return Collectors::NullCollector.new
        collector.match(tag)
      end
    end

  end
end
