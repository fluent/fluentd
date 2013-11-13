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

    require 'yajl'

    class ExceptionOutput < Output
      Plugin.register_output('exception', self)

      def configure(conf)
        super
      end

      def emit(tag, time, record)
        raise "exception '#{tag}' #{time} #{Yajl.dump(ecord)}"
      end
    end

  end
end
