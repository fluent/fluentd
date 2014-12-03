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
  require 'json'

  module Config
    def self.size_value(str)
      case str.to_s
      when /([0-9]+)k/i
        $~[1].to_i * 1024
      when /([0-9]+)m/i
        $~[1].to_i * (1024 ** 2)
      when /([0-9]+)g/i
        $~[1].to_i * (1024 ** 3)
      when /([0-9]+)t/i
        $~[1].to_i * (1024 ** 4)
      else
        str.to_i
      end
    end

    def self.time_value(str)
      case str.to_s
      when /([0-9]+)s/
        $~[1].to_i
      when /([0-9]+)m/
        $~[1].to_i * 60
      when /([0-9]+)h/
        $~[1].to_i * 60 * 60
      when /([0-9]+)d/
        $~[1].to_i * 24 * 60 * 60
      else
        str.to_f
      end
    end

    def self.bool_value(str)
      return nil if str.nil?
      case str.to_s
      when 'true', 'yes'
        true
      when 'false', 'no'
        false
      when ''
        true
      else
        nil
      end
    end
  end

  Configurable.register_type(:string, Proc.new { |val, opts|
    val
  })

  Configurable.register_type(:integer, Proc.new { |val, opts|
    val.to_i
  })

  Configurable.register_type(:float, Proc.new { |val, opts|
    val.to_f
  })

  Configurable.register_type(:size, Proc.new { |val, opts|
    Config.size_value(val)
  })

  Configurable.register_type(:bool, Proc.new { |val, opts|
    Config.bool_value(val)
  })

  Configurable.register_type(:time, Proc.new { |val, opts|
    Config.time_value(val)
  })

  Configurable.register_type(:hash, Proc.new { |val, opts|
    param = val.is_a?(String) ? JSON.load(val) : val
    if param.class != Hash
      raise ConfigError, "hash required but got #{val.inspect}"
    end
    param
  })

  Configurable.register_type(:array, Proc.new { |val, opts|
    param = val.is_a?(String) ? JSON.load(val) : val
    if param.class != Array
      raise ConfigError, "array required but got #{val.inspect}"
    end
    param
  })
end
