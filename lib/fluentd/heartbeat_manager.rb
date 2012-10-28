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

  class HeartbeatManager
    def initialize
      @rpipes = {}
      @heartbeat_time = {}
    end

    def new_client(name)
      rpipe, wpipe = IO.pipe
      rpipe.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
      rpipe.sync = true
      wpipe.sync = true
      @rpipes[rpipe] = name
      @heartbeat_time[name] = Time.now.to_i
      return Client.new(wpipe)
    end

    def last_heartbeat_time(name)
      @heartbeat_time[name]
    end

    def close
      @rpipes.each_pair {|c,name|
        c.close rescue nil
      }
      @rpipes.clear
      @heartbeat_time.clear
    end

    def receive(timeout)
      ready_pipes, _, _ = IO.select(@rpipes.keys, nil, nil, timeout)
      unless ready_pipes
        return nil
      end

      ready_pipes.each {|c|
        begin
          data = c.read_nonblock(32*1024)
        rescue Errno::EAGAIN, Errno::EINTR
          next
        rescue EOFError
          name = @rpipes.delete(c)
          @heartbeat_time.delete(name)
          c.close rescue nil
          next
        end

        now ||= Time.now.to_i

        name = @rpipes[c]
        @heartbeat_time[name] = now
      }
      nil
    end

    HEARTBEAT_MESSAGE = [0].pack('C')

    class Client
      def initialize(wpipe)
        @wpipe = wpipe
      end

      def heartbeat
        @wpipe.write HEARTBEAT_MESSAGE
      end

      def close
        @wpipe.close rescue nil
      end
    end
  end

end
