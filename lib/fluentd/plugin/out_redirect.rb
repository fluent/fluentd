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

    require_relative '../collectors/label_redirect_collector'

    class RedirectOutput < Output
      Plugin.register_output('redirect', self)

      def configure(conf)
        super

        @label = conf['label']
        @tag = conf['tag']

        self.default_collector = Collectors::LabelRedirectCollector.new(root_agent, @label)
      end

      def emit(tag, time, record)
        collector.emit(@tag || tag, time, record)
      end

      def emits(tag, es)
        collector.emits(@tag || tag, es)
      end
    end

  end
end
