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

  class NoNameUNIXServer < IOExchange::Server
    def initialize
      super
      @mutex = Mutex.new
      @cond = ConditionVariable.new
      @backlog = []
    end

    def open_io(msg)
      s1, s2 = UNIXSocket.pair
      @mutex.synchronize do
        @backlog << s1
        @cond.broadcast
      end
      return s2
    end

    def accept
      @mutex.synchronize do
        while @backlog.empty?
          if finished?
            return nil
          end
          @cond.wait(@mutex, 0.5)
        end
        return @backlog.shift
      end
    end

    def new_client
      return Client.new(new_connection, connection_mutex)
    end

    class Client < IOExchange::Client
      def connect
        return open_io(nil)
      end
    end
  end

end
