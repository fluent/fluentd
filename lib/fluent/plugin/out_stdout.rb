#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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
  class StdoutOutput < Output
    Plugin.register_output('stdout', self)

    OUTPUT_PROCS = {
      :json => Proc.new {|record| Yajl.dump(record) },
      :hash => Proc.new {|record| record.to_s },
    }

    config_param :output_type, :default => :json do |val|
      case val.downcase
      when 'json'
        :json
      when 'hash'
        :hash
      else
        raise ConfigError, "stdout output output_type should be 'json' or 'hash'"
      end
    end

    def configure(conf)
      super
      @output_proc = OUTPUT_PROCS[@output_type]
    end

    def emit(tag, es, chain)
      es.each {|time,record|
        $log.write "#{Time.at(time).localtime} #{tag}: #{@output_proc.call(record)}\n"
      }
      $log.flush

      chain.next
    end
  end
end
