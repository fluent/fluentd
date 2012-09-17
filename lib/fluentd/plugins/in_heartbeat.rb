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

    class HeartbeatInput < Inputs::BasicInput

      Plugin.register_input(:heartbeat, self)

      def initialize
        super
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

      def stop
        @finish_flag.set!
        if @thread
          @thread.join
          @thread = nil
        end
      end

      def shutdown
        #stop
      end

      private
      def run
        until @finish_flag.set?
          begin
            w = stream_source.open(@tag)
            begin
              w.append(Time.now.to_i, @message)
            ensure
              w.close
            end
          rescue
            puts "error: #{$!}"
            $!.backtrace.each {|bt| puts "  #{bt}" }
          end
          @finish_flag.wait(1)
        end
      end
    end

  end
end

