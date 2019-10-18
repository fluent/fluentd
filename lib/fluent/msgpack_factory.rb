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

require 'msgpack'
require 'fluent/time'

module Fluent
  module MessagePackFactory
    @@engine_factory = nil

    module Mixin
      def msgpack_factory
        MessagePackFactory.engine_factory
      end

      def msgpack_packer(*args)
        MessagePackFactory.msgpack_packer(*args)
      end

      def msgpack_unpacker(*args)
        MessagePackFactory.msgpack_unpacker(*args)
      end
    end

    def self.engine_factory
      @@engine_factory || factory
    end

    def self.msgpack_packer(*args)
      engine_factory.packer(*args)
    end

    def self.msgpack_unpacker(*args)
      engine_factory.unpacker(*args)
    end

    def self.factory
      factory = MessagePack::Factory.new
      factory.register_type(Fluent::EventTime::TYPE, Fluent::EventTime)
      factory
    end

    def self.packer(*args)
      factory.packer(*args)
    end

    def self.unpacker(*args)
      factory.unpacker(*args)
    end

    def self.init
      factory = MessagePack::Factory.new
      factory.register_type(Fluent::EventTime::TYPE, Fluent::EventTime)
      @@engine_factory = factory
    end

    def self.thread_local_msgpack_packer
      Thread.current[:local_msgpack_packer] ||= MessagePackFactory.engine_factory.packer
    end

    def self.thread_local_msgpack_unpacker
      Thread.current[:local_msgpack_unpacker] ||= MessagePackFactory.engine_factory.unpacker
    end
  end
end
