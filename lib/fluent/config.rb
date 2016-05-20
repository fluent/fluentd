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

module Fluent
  module Config
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
