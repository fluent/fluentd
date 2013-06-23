#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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
module Fluentd

  class ConfigError < StandardError
  end

  class ConfigParseError < ConfigError
  end

  module Config
    require 'strscan'

    here = File.expand_path(File.dirname(__FILE__))

    {
      :Context => 'config/context',
      :Element => 'config/element',
      :BasicParser => 'config/basic_parser',
      :ParserModule => 'config/basic_parser',
      :LiteralParser => 'config/literal_parser',
      :Parser => 'config/parser',
      :CompatParser => 'config/compat_parser',
    }.each_pair {|k,v|
      autoload k, File.join(here, v)
    }

    module ClassMethods
      extend Forwardable
      def_delegators :'Fluentd::Config::Parser', :read, :parse
    end

    extend ClassMethods
  end

end
