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

require 'fluent/output'

module Fluent
  class StdoutOutput < Output
    Plugin.register_output('stdout', self)

    desc 'Output format.(json,hash)'
    config_param :output_type, default: 'json'

    def configure(conf)
      super
      @formatter = Plugin.new_formatter(@output_type)
      @formatter.configure(conf)
    end

    def emit(tag, es, chain)
      es.each {|time,record|
        $log.write "#{Time.at(time).localtime} #{tag}: #{@formatter.format(tag, time, record).chomp}\n"
      }
      $log.flush

      chain.next
    end
  end
end
