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
  module Plugins

    class ForwardInput < Inputs::BasicInput
      Plugin.register_input('forward', self)

      def initialize
        super
      end

      config_param :port, :integer, :default => 24224
      config_param :bind, :string, :default => '0.0.0.0'

      def start
        actor.create_tcp_thread_server(@bind, @port, &method(:thread_main))
        super
      end

      def thread_main(io)
        u = MessagePack::Unpacker.new(io)
        u.each do |msg|
          on_message(msg, io)
        end
      ensure
        # TODO log
      end

      # message Entry {
      #   1: long time
      #   2: object record
      # }
      #
      # message Forward {
      #   1: string tag
      #   2: list<Entry> entries
      # }
      #
      # message PackedForward {
      #   1: string tag
      #   2: raw entries  # msgpack stream of Entry
      # }
      #
      # message Message {
      #   1: string tag
      #   2: long? time
      #   3: object record
      # }
      def on_message(msg, io)
        tag = msg[0].to_s
        entries = msg[1]

        if entries.class == String
          # PackedForward
          stream_source.open(tag) do |w|
            u = MessagePack::Unpacker.new
            u.feed_each(entries) do |time,record|
              w.append(time, record)
            end
          end

        elsif entries.class == Array
          # Forward
          stream_source.open(tag) do |w|
            entries.each {|e|
              time = e[0].to_i
              time = (now ||= Engine.now) if time == 0
              record = e[1]
              w.append(time, record)
            }
          end

        else
          # Message
          time = msg[1]
          time = Engine.now if time == 0
          record = msg[2]
          stream_source.append(tag, time, record)

        end
      end
    end

  end
end

