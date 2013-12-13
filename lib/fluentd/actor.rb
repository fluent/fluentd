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

  require 'coolio'

  class Actor
    def initialize
      @loop = Coolio::Loop.new
    end

    attr_reader :loop

    def start
      if @loop.has_active_watchers?
        @thread = Thread.new(&method(:run))
      end
      nil
    end

    def run
      @loop.run
      nil
    rescue => e
      Engine.log.error e.to_s
      Engine.log.error_backtrace e.backtrace
      sleep 1  # TODO auto restart?
      retry
    end

    def stop
      @loop.watchers.each {|w| w.detach }
      @loop.stop if @thread
      nil
    end

    def join
      @thread.join if @thread
      nil
    end

    require 'fluentd/actors/background_actor'
    include Actors::BackgroundActor

    require 'fluentd/actors/timer_actor'
    include Actors::TimerActor

    require 'fluentd/actors/io_actor'
    include Actors::IOActor

    require 'fluentd/actors/socket_actor'
    include Actors::SocketActor

    # TODO not implemented yet
    #require 'fluentd/actors/async_actor'
    #include Actors::AsyncActor

    module AgentMixin
      def initialize
        @actor = Actor.new
        super
      end

      attr_reader :actor

      def start
        super
        @actor.start
      end

      def shutdown
        @actor.stop
        @actor.join
        super
      end
    end
  end

end
