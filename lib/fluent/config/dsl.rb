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

require 'json'

require 'fluent/config'
require 'fluent/config/element'

module Fluent
  module Config
    module DSL
      RESERVED_PARAMETERS = [:type, :id, :log_level, :label] # Need '@' prefix for reserved parameters

      module Parser
        def self.read(path)
          path = File.expand_path(path)
          data = File.read(path)
          parse(data, path)
        end

        def self.parse(source, source_path="config.rb")
          Proxy.new('ROOT', nil).eval(source, source_path).to_config_element
        end
      end

      class Proxy
        def initialize(name, arg, include_basepath = Dir.pwd)
          @element = Element.new(name, arg, self)
          @include_basepath = include_basepath
        end

        def element
          @element
        end

        def include_basepath
          @include_basepath
        end

        def eval(source, source_path)
          @element.instance_eval(source, source_path)
          self
        end

        def to_config_element
          @element.instance_eval do
            Config::Element.new(@name, @arg, @attrs, @elements)
          end
        end

        def add_element(name, arg, block)
          ::Kernel.raise ::ArgumentError, "#{name} block must be specified" if block.nil?

          proxy = self.class.new(name.to_s, arg)
          proxy.element.instance_exec(&block)

          @element.instance_eval do
            @elements.push(proxy.to_config_element)
          end

          self
        end
      end

      class Element < BasicObject
        def initialize(name, arg, proxy)
          @name     = name
          @arg      = arg || ''
          @attrs    = {}
          @elements = []
          @proxy    = proxy
        end

        def to_int
          __id__
        end

        def method_missing(name, *args, &block)
          ::Kernel.raise ::ArgumentError, "Configuration DSL Syntax Error: only one argument allowed" if args.size > 1
          value = args.first

          if block
            proxy = Proxy.new(name.to_s, value)
            proxy.element.instance_exec(&block)
            @elements.push(proxy.to_config_element)
          else
            param_name = RESERVED_PARAMETERS.include?(name) ? "@#{name}" : name.to_s
            @attrs[param_name] = if value.is_a?(Array) || value.is_a?(Hash)
                                   JSON.dump(value)
                                 else
                                   value.to_s
                                 end
          end

          self
        end

        def include(*args)
          ::Kernel.raise ::ArgumentError, "#{name} block requires arguments for include path" if args.nil? || args.size != 1
          if args.first =~ /\.rb$/
            path = File.expand_path(args.first)
            data = File.read(path)
            self.instance_eval(data, path)
          else
            ss = StringScanner.new('')
            Config::V1Parser.new(ss, @proxy.include_basepath, '', nil).eval_include(@attrs, @elements, args.first)
          end
        end

        def source(&block)
          @proxy.add_element('source', nil, block)
        end

        def match(*args, &block)
          ::Kernel.raise ::ArgumentError, "#{name} block requires arguments for match pattern" if args.nil? || args.size != 1
          @proxy.add_element('match', args.first, block)
        end

        def self.const_missing(name)
          return ::Kernel.const_get(name) if ::Kernel.const_defined?(name)

          if name.to_s =~ /^Fluent::Config::DSL::Element::(.*)$/
            name = "#{$1}".to_sym
            return ::Kernel.const_get(name) if ::Kernel.const_defined?(name)
          end
          ::Kernel.eval("#{name}")
        end

        def ruby(&block)
          if block
            @proxy.instance_exec(&block)
          else
            ::Kernel
          end
        end
      end
    end
  end
end
