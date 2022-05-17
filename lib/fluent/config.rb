#
# Fluent
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

require 'fluent/config/error'
require 'fluent/config/element'
require 'fluent/configurable'
require 'fluent/config/yaml_parser'

module Fluent
  module Config
    # @param config_path [String] config file path
    # @param encoding [String] encoding of config file
    # @param additional_config [String] config which is added to last of config body
    # @param use_v1_config [Bool] config is formatted with v1 or not
    # @return [Fluent::Config]
    def self.build(config_path:, encoding: 'utf-8', additional_config: nil, use_v1_config: true, type: nil)
      config_file_ext = File.extname(config_path)
      if config_file_ext == '.yaml' || config_file_ext == '.yml'
        type = :yaml
      end

      if type == :yaml || type == :yml
        return Fluent::Config::YamlParser.parse(config_path)
      end

      config_fname = File.basename(config_path)
      config_basedir = File.dirname(config_path)
      config_data = File.open(config_path, "r:#{encoding}:utf-8") do |f|
        s = f.read
        if additional_config
          c = additional_config.gsub("\\n", "\n")
          s += "\n#{c}"
        end
        s
      end

      Fluent::Config.parse(config_data, config_fname, config_basedir, use_v1_config)
    end

    def self.parse(str, fname, basepath = Dir.pwd, v1_config = nil, syntax: :v1)
      parser = if fname =~ /\.rb$/ || syntax == :ruby
                 :ruby
               elsif v1_config.nil?
                 case syntax
                 when :v1 then :v1
                 when :v0 then :v0
                 else
                   raise ArgumentError, "Unknown Fluentd configuration syntax: '#{syntax}'"
                 end
               elsif v1_config then :v1
               else :v0
               end
      case parser
      when :v1
        require 'fluent/config/v1_parser'
        V1Parser.parse(str, fname, basepath, Kernel.binding)
      when :v0
        # TODO: show deprecated message in v1
        require 'fluent/config/parser'
        Parser.parse(str, fname, basepath)
      when :ruby
        require 'fluent/config/dsl'
        $log.warn("Ruby DSL configuration format is deprecated. Please use original configuration format. https://docs.fluentd.org/configuration/config-file") if $log
        Config::DSL::Parser.parse(str, File.join(basepath, fname))
      else
        raise "[BUG] unknown configuration parser specification:'#{parser}'"
      end
    end

    def self.new(name = '')
      Element.new(name, '', {}, [])
    end
  end
end
