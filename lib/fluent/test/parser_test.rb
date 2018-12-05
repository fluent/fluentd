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

require 'fluent/parser'
require 'fluent/config'

module Fluent
  module Test
    class ParserTestDriver
      def initialize(klass_or_str, format=nil, conf={}, &block)
        if klass_or_str.is_a?(Class)
          if block
            # Create new class for test w/ overwritten methods
            #   klass.dup is worse because its ancestors does NOT include original class name
            klass_name = klass_or_str.name
            klass_or_str = Class.new(klass_or_str)
            klass_or_str.define_singleton_method("name") { klass_name }
            klass_or_str.module_eval(&block)
          end
          case klass_or_str.instance_method(:initialize).arity
          when 0
            @instance = klass_or_str.new
          when -2
            # for RegexpParser
            @instance = klass_or_str.new(format, conf)
          end
        elsif klass_or_str.is_a?(String)
          @instance = Fluent::Plugin.new_parser(klass_or_str)
        else
          @instance = klass_or_str
        end
        @config = Config.new
      end

      attr_reader :instance, :config

      def configure(conf)
        case conf
        when Fluent::Config::Element
          @config = conf
        when String
          @config = Config.parse(conf, 'fluent.conf')
        when Hash
          @config = Config::Element.new('ROOT', '', conf, [])
        else
          raise "Unknown type... #{conf}"
        end
        @instance.configure(@config)
        self
      end

      def parse(text, &block)
        @instance.parse(text, &block)
      end
    end
  end
end
