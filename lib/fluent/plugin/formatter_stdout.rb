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

require 'fluent/plugin/formatter'

module Fluent
  module Plugin
    class StdoutFormatter < Formatter
      Plugin.register_formatter('stdout', self)

      TIME_FORMAT = '%Y-%m-%d %H:%M:%S.%N %z'

      config_param :output_type, :string, default: 'json'

      def configure(conf)
        super

        @time_formatter = Strftime.new(TIME_FORMAT)
        @sub_formatter = Plugin.new_formatter(@output_type, parent: self.owner)
        @sub_formatter.configure(conf)
      end

      def start
        super
        @sub_formatter.start
      end

      def format(tag, time, record)
        "#{@time_formatter.exec(Time.at(time).localtime)} #{tag}: #{@sub_formatter.format(tag, time, record).chomp}\n"
      end

      def stop
        @sub_formatter.stop
        super
      end

      def before_shutdown
        @sub_formatter.before_shutdown
        super
      end

      def shutdown
        @sub_formatter.shutdown
        super
      end

      def after_shutdown
        @sub_formatter.after_shutdown
        super
      end

      def close
        @sub_formatter.close
        super
      end

      def terminate
        @sub_formatter.terminate
        super
      end
    end
  end
end
