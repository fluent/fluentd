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

require 'fluent/plugin'
require 'fluent/plugin/parser'
require 'fluent/config/element'
require 'fluent/configurable'

module Fluent
  module PluginHelper
    module Parser
      def parser_create(usage: '', type: nil, conf: nil, default_type: nil)
        parser = @_parsers[usage]
        return parser if parser && !type && !conf

        type = if type
                 type
               elsif conf && conf.respond_to?(:[])
                 raise Fluent::ConfigError, "@type is required in <parse>" unless conf['@type']
                 conf['@type']
               elsif default_type
                 default_type
               else
                 raise ArgumentError, "BUG: both type and conf are not specified"
               end
        parser = Fluent::Plugin.new_parser(type, parent: self)
        config = case conf
                 when Fluent::Config::Element
                   conf
                 when Hash
                   # in code, programmer may use symbols as keys, but Element needs strings
                   conf = Hash[conf.map{|k,v| [k.to_s, v]}]
                   Fluent::Config::Element.new('parse', usage, conf, [])
                 when nil
                   Fluent::Config::Element.new('parse', usage, {}, [])
                 else
                   raise ArgumentError, "BUG: conf must be a Element, Hash (or unspecified), but '#{conf.class}'"
                 end
        parser.configure(config)
        if @_parsers_started
          parser.start
        end

        @_parsers[usage] = parser
        parser
      end

      module ParserParams
        include Fluent::Configurable
        # minimum section definition to instantiate parser plugin instances
        config_section :parse, required: false, multi: true, init: true, param_name: :parser_configs do
          config_argument :usage, :string, default: ''
          config_param    :@type, :string # config_set_default required for :@type
        end
      end

      def self.included(mod)
        mod.include ParserParams
      end

      attr_reader :_parsers # for tests

      def initialize
        super
        @_parsers_started = false
        @_parsers = {} # usage => parser
      end

      def configure(conf)
        super

        @parser_configs.each do |section|
          if @_parsers[section.usage]
            raise Fluent::ConfigError, "duplicated parsers configured: #{section.usage}"
          end
          parser = Plugin.new_parser(section[:@type], parent: self)
          parser.configure(section.corresponding_config_element)
          @_parsers[section.usage] = parser
        end
      end

      def start
        super
        @_parsers_started = true
        @_parsers.each_pair do |usage, parser|
          parser.start
        end
      end

      def parser_operate(method_name, &block)
        @_parsers.each_pair do |usage, parser|
          begin
            parser.send(method_name)
            block.call(parser) if block_given?
          rescue => e
            log.error "unexpected error while #{method_name}", usage: usage, parser: parser, error: e
          end
        end
      end

      def stop
        super
        parser_operate(:stop)
      end

      def before_shutdown
        parser_operate(:before_shutdown)
        super
      end

      def shutdown
        parser_operate(:shutdown)
        super
      end

      def after_shutdown
        parser_operate(:after_shutdown)
        super
      end

      def close
        parser_operate(:close)
        super
      end

      def terminate
        parser_operate(:terminate)
        @_parsers_started = false
        @_parsers = {}
        super
      end
    end
  end
end
