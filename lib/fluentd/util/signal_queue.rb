#
# Fluentd
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
      @mutex = Mutex.new
      @cond = ConditionVariable.new

      block.call(self) if block
    end

    def trap(sig, &block)
      sig = sig.to_sym
      old = @handlers[sig]

      Kernel.trap(sig) do
        enqueue(sig)
      end

      @handlers[sig] = block
      old
    end

    def start
      @thread = Thread.new(&method(:run))
    end

    def join
      @thread.join
    end

    def stop
      enqueue(nil)
    end

    def shutdown
      stop
      join
    end

    def run
      finished = false
      until finished
        h = nil
        @mutex.synchronize do
          while @queue.empty?
            @cond.wait(@mutex)
          end
          sig = @queue.shift
          if sig == nil
            finished = true
          else
            h = @handlers[sig]
          end
        end

        begin
          h.call if h
        rescue
          STDERR.print "#{$!}\n"
          $!.backtrace.each {|bt|
            STDERR.print "\t#{bt}\n"
            STDERR.flush
          }
        end
      end
    end

    private
    def enqueue(sig)
      if Thread.current == self
        @queue << sig
        if @mutex.locked?
          @cond.signal
        end
      else
        @mutex.synchronize do
          @queue << sig
          @cond.signal
        end
      end
    end
  end

end
