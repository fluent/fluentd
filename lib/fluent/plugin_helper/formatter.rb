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
require 'fluent/plugin/formatter'
require 'fluent/config/element'
require 'fluent/configurable'

module Fluent
  module PluginHelper
    module Formatter
      def formatter_create(usage: '', type: nil, conf: nil, default_type: nil)
        formatter = @_formatters[usage]
        return formatter if formatter && !type && !conf

        type = if type
                 type
               elsif conf && conf.respond_to?(:[])
                 raise Fluent::ConfigError, "@type is required in <format>" unless conf['@type']
                 conf['@type']
               elsif default_type
                 default_type
               else
                 raise ArgumentError, "BUG: both type and conf are not specified"
               end
        formatter = Fluent::Plugin.new_formatter(type, parent: self)
        config = case conf
                 when Fluent::Config::Element
                   conf
                 when Hash
                   # in code, programmer may use symbols as keys, but Element needs strings
                   conf = Hash[conf.map{|k,v| [k.to_s, v]}]
                   Fluent::Config::Element.new('format', usage, conf, [])
                 when nil
                   Fluent::Config::Element.new('format', usage, {}, [])
                 else
                   raise ArgumentError, "BUG: conf must be a Element, Hash (or unspecified), but '#{conf.class}'"
                 end
        formatter.configure(config)
        if @_formatters_started
          formatter.start
        end

        @_formatters[usage] = formatter
        formatter
      end

      module FormatterParams
        include Fluent::Configurable
        # minimum section definition to instantiate formatter plugin instances
        config_section :format, required: false, multi: true, init: true, param_name: :formatter_configs do
          config_argument :usage, :string, default: ''
          config_param    :@type, :string # config_set_default required for :@type
        end
      end

      def self.included(mod)
        mod.include FormatterParams
      end

      attr_reader :_formatters # for tests

      def initialize
        super
        @_formatters_started = false
        @_formatters = {} # usage => formatter
      end

      def configure(conf)
        super

        @formatter_configs.each do |section|
          if @_formatters[section.usage]
            raise Fluent::ConfigError, "duplicated formatter configured: #{section.usage}"
          end
          formatter = Plugin.new_formatter(section[:@type], parent: self)
          formatter.configure(section.corresponding_config_element)
          @_formatters[section.usage] = formatter
        end
      end

      def start
        super
        @_formatters_started = true
        @_formatters.each_pair do |usage, formatter|
          formatter.start
        end
      end

      def formatter_operate(method_name, &block)
        @_formatters.each_pair do |usage, formatter|
          begin
            formatter.send(method_name)
            block.call(formatter) if block_given?
          rescue => e
            log.error "unexpected error while #{method_name}", usage: usage, formatter: formatter, error: e
          end
        end
      end

      def stop
        super
        formatter_operate(:stop)
      end

      def before_shutdown
        formatter_operate(:before_shutdown)
        super
      end

      def shutdown
        formatter_operate(:shutdown)
        super
      end

      def after_shutdown
        formatter_operate(:after_shutdown)
        super
      end

      def close
        formatter_operate(:close)
        super
      end

      def terminate
        formatter_operate(:terminate)
        @_formatters_started = false
        @_formatters = {}
        super
      end
    end
  end
end
