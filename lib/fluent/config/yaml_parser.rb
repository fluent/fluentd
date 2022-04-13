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

require 'fluent/config/yaml_parser/loader'
require 'fluent/config/yaml_parser/parser'
require 'pathname'

module Fluent
  module Config
    module YamlParser
      def self.parse(path)
        context = Kernel.binding

        unless context.respond_to?(:use_nil)
          context.define_singleton_method(:use_nil) do
            raise Fluent::SetNil
          end
        end

        unless context.respond_to?(:use_default)
          context.define_singleton_method(:use_default) do
            raise Fluent::SetDefault
          end
        end

        unless context.respond_to?(:hostname)
          context.define_singleton_method(:hostname) do
            Socket.gethostname
          end
        end

        unless context.respond_to?(:worker_id)
          context.define_singleton_method(:worker_id) do
            ENV['SERVERENGINE_WORKER_ID'] || ''
          end
        end

        s = Fluent::Config::YamlParser::Loader.new(context).load(Pathname.new(path))
        Fluent::Config::YamlParser::Parser.new(s).build.to_element
      end
    end
  end
end
