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

module Fluent
  class ConfigError < StandardError
  end

  class ConfigParseError < ConfigError
  end

  class ObsoletedParameterError < ConfigError
  end

  class SetNil < Exception
  end

  class SetDefault < Exception
  end

  class NotFoundPluginError < ConfigError
    attr_reader :type, :kind

    def initialize(msg, type: nil, kind: nil)
      @msg = msg
      @type = type
      @kind = kind

      super(msg)
    end
  end
end
