#
# SignalQueue
#
# Copyright (C) 2012 FURUHASHI Sadayuki
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

  class SignalQueue
    def self.start(&block)
      st = new(&block)
      st.start
      return st
    end

    def initialize(&block)
      require 'thread'

      @handlers = {}
      @queue = []
      @finished = false

      block.call(self) if block
    end

    def trap(sig, command=nil, &block)
      sig = sig.to_sym
      old = @handlers[sig]

      if block
        Kernel.trap(sig) do
          enqueue(sig)
        end
        @handlers[sig] = block

      else
        Kernel.trap(sig, command)
        @handlers.delete(sig)
      end

      old
    end

    def start
      @thread = Thread.new(&method(:run))
    end

    def join
      @thread.join
    end

    def stop
      @finished = true
      self
    end

    def shutdown
      stop
      join
    end

    def run
      @owner_thread = Thread.current
      until @finished
        sleep 0.5

        while sig = @queue.shift
          h = @handlers[sig]
          next unless h

          begin
            h.call
          rescue
            STDERR.print "#{$!}\n"
            $!.backtrace.each {|bt|
              STDERR.print "\t#{bt}\n"
              STDERR.flush
            }
          end
        end
      end
    end

    private
    def enqueue(sig)
      @queue << sig
    end
  end

end
