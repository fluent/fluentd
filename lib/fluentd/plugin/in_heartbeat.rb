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
  module Builtin

    class HeartbeatInput < Agent
      # TODO create base class for Input plugins
      include StreamSource

      Plugin.register_input(:heartbeat, self)

      def initialize
        @mutex = Mutex.new
        @finish_flag = BlockingFlag.new
      end

      def configure(conf)
        @tag = conf['tag'] || 'heartbeat'

        json = conf['message'] || '{"heartbeat":1}'
        @message = JSON.load(json)
      end

      def start
        @thread = Thread.new(&method(:run))
      end

      def open(tag, &block)
        @mutex.synchronize do
          @tag = tag
          yield self
        end
      end

      def append(time, record)
        puts "#{time} #{@tag} #{record.to_json}"
      end

      def write(chunk)
        chunk.each(&method(:append))
      end

      def shutdown
      end

      private
      def run
        until @finish_flag.set?
          begin
            stream_source.open(@tag) do |w|
              w.append(Time.now.to_i, @message)
            end
          rescue
            puts "error: #{$!}"
            $!.backtrace.each {|bt| puts "  #{bt}" }
          end
          sleep 1
        end
      end
    end

  end
end

