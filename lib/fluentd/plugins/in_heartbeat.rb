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

      config_param :tag, :string
      config_param :message, :hash

      def start
        actor.every 0.5, &method(:emit_message)
        super
      end

      private

      def emit_message
        w = stream_source.open(@tag)
        begin
          w.append(Time.now.to_i, @message)
        ensure
          w.close
        end
      rescue
        @log.error "emit error: #{$!}"
        @log.warn_backtrace
      end
    end

  end
end

