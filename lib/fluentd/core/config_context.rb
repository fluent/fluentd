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

  module ConfigContext
    def self.inject!(conf)
      m = ConfigContext.dup
      c = Context.new(m)
      m.module_eval do
        define_method(:context) { c }
      end
      extend_recursive(conf, m)
      conf
    end

    def self.extend_recursive(obj, mod)
      obj.extend(mod)
      obj.elements.each {|e|
        extend_recursive(e, mod)
      }
      obj
    end

    class Context
      def initialize(mod)
        @mod = mod
      end

      def method_missing(action, *args, &block)
        m = /\A(.*)=\z/.match(action.to_s)
        super unless m
        name = m[1].to_sym

        if args.length != 1
          raise ArgumentError, "wrong number of arguments(#{args.length} for 1)"
        end
        obj = args[0]

        @mod.module_eval do
          define_method(name) { obj }
        end
      end
    end
  end

end

