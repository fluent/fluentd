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

module Fluent
  require 'fluent/config/error'
  require 'fluent/config/element'

  module Config
    def self.parse(str, fname, basepath = Dir.pwd, v1_config = false)
      if fname =~ /\.rb$/
        require 'fluent/config/dsl'
        Config::DSL::Parser.parse(str, File.join(basepath, fname))
      else
        if v1_config
          require 'fluent/config/v1_parser'
          V1Parser.parse(str, fname, basepath, Kernel.binding)
        else
          require 'fluent/config/parser'
          Parser.parse(str, fname, basepath)
        end
      end
    end

    def self.new(name = '')
      Element.new(name, '', {}, [])
    end
  end
end
