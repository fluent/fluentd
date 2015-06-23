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

require 'fluent/config'
require 'fluent/config/error'

module Fluent
  module Config
    module DSL
      module Parser
        def self.read(path)
          path = File.expand_path(path)
          data = File.read(path)
          parse(data, path)
        end

        def self.parse(source, source_path="config.rb")
          confpath = File.dirname(File.expand_path(source_path))
          endres = parse_include(source,confpath,[])
          Proxy.new('ROOT', nil).eval(source, source_path).to_config_element
        end
        def self.parse_include(content,confpath=nil,attrs=[])
          attrs.push(content)
          require 'httpclient'
          clt = HTTPClient.new
          res = /(include|@include).*/m.match(content).to_s
          res.split("\n").each do |i|
            content.gsub!(i,"")
            i.strip!
            if ! i.start_with?("#")
              url=i.split(%r{\s+})[1]
              url.gsub!("\"","")
              u = URI.parse(url)
              if u.scheme == 'file' || u.path == url

                path = u.path
                if path[0] != ?/ and path[-1] == ?/
                    pattern = File.join(File.join(confpath,path),"*.rb")
                elsif path[0] == ?/ and  FileTest::directory? path
                    pattern = File.join(path,"*.rb")
                elsif path[0] == ?/ and path[-1] != ?/  and FileTest::file? path
                    pattern = path
              end
                files = Dir.glob(pattern.to_s)
                if ! files.length.eql?(0) then
                  files.sort.each { |path|
                  t = File.read(path)
                  dirpath = File.dirname(path)
                  attrs.push(t.force_encoding('utf-8'))
                  parse_include(t,dirpath,attrs)
                  }
                else
                  raise ConfigParseError, "include error #{pattern} not exists!"
                end
            else
              $log.info url,"parse_include"
              out = clt.get(url)
              attrs.push(out.content.force_encoding('utf-8'))
              parse_include(out.content.force_encoding('utf-8'),nil,attrs=attrs)
            end
          end
          return attrs.uniq.join("\n")
          rescue => e
            raise ConfigParseError, "about include #{confpath} error "
        end
      end
      class Proxy
        def initialize(name, arg)
          @element = Element.new(name, arg, self)
        end

        def element
          @element
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

        def method_missing(name, *args, &block)
          ::Kernel.raise ::ArgumentError, "Configuration DSL Syntax Error: only one argument allowed" if args.size > 1
          value = args.first

          if block
            proxy = Proxy.new(name.to_s, value)
            proxy.element.instance_exec(&block)
            @elements.push(proxy.to_config_element)
          else
            @attrs[name.to_s] = if value.is_a?(Array) || value.is_a?(Hash)
                                  JSON.dump(value)
                                else
                                  value.to_s
                                end
          end

          self
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
