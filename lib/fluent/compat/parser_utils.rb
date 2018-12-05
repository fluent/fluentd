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

require 'fluent/plugin_helper/compat_parameters'

module Fluent
  module Compat
    module ParserUtils
      PARSER_PARAMS = Fluent::PluginHelper::CompatParameters::PARSER_PARAMS

      def self.convert_parser_conf(conf)
        return if conf.elements(name: 'parse').first

        parser_params = {}
        PARSER_PARAMS.each do |older, newer|
          next unless newer
          if conf.has_key?(older)
            parser_params[newer] = conf[older]
          end
        end
        unless parser_params.empty?
          conf.elements << Fluent::Config::Element.new('parse', '', parser_params, [])
        end
      end
    end
  end
end
