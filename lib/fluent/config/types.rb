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

require 'json'

require 'fluent/config/error'

module Fluent
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
        # Current parser passes comment without actual values, e.g. "param #foo".
        # parser should pass empty string in this case but changing behaviour may break existing environment so keep parser behaviour. Just ignore comment value in boolean handling for now.
        if str.respond_to?('start_with?') && str.start_with?('#')
          true
        else
          nil
        end
      end
    end

    def self.regexp_value(str)
      return nil unless str
      return Regexp.compile(str) unless str.start_with?("/")
      right_slash_position = str.rindex("/")
      if right_slash_position < str.size - 3
        raise Fluent::ConfigError, "invalid regexp: missing right slash: #{str}"
      end
      options = str[(right_slash_position + 1)..-1]
      option = 0
      option |= Regexp::IGNORECASE if options.include?("i")
      option |= Regexp::MULTILINE if options.include?("m")
      Regexp.compile(str[1...right_slash_position], option)
    end

    STRING_TYPE = Proc.new { |val, opts|
      v = val.to_s
      v = v.frozen? ? v.dup : v # config_param can't assume incoming string is mutable
      v.force_encoding(Encoding::UTF_8)
    }
    ENUM_TYPE = Proc.new { |val, opts|
      s = val.to_sym
      list = opts[:list]
      raise "Plugin BUG: config type 'enum' requires :list of symbols" unless list.is_a?(Array) && list.all?{|v| v.is_a? Symbol }
      unless list.include?(s)
        raise ConfigError, "valid options are #{list.join(',')} but got #{val}"
      end
      s
    }
    INTEGER_TYPE = Proc.new { |val, opts| val.to_i }
    FLOAT_TYPE = Proc.new { |val, opts| val.to_f }
    SIZE_TYPE = Proc.new { |val, opts| Config.size_value(val) }
    BOOL_TYPE = Proc.new { |val, opts| Config.bool_value(val) }
    TIME_TYPE = Proc.new { |val, opts| Config.time_value(val) }
    REGEXP_TYPE = Proc.new { |val, opts| Config.regexp_value(val) }

    REFORMAT_VALUE = ->(type, value) {
      if value.nil?
        value
      else
        case type
        when :string  then value.to_s.force_encoding(Encoding::UTF_8)
        when :integer then value.to_i
        when :float   then value.to_f
        when :size then Config.size_value(value)
        when :bool then Config.bool_value(value)
        when :time then Config.time_value(value)
        when :regexp then Config.regexp_value(value)
        else
          raise "unknown type in REFORMAT: #{type}"
        end
      end
    }

    HASH_TYPE = Proc.new { |val, opts|
      param = if val.is_a?(String)
                val.start_with?('{') ? JSON.load(val) : Hash[val.strip.split(/\s*,\s*/).map{|v| v.split(':', 2)}]
              else
                val
              end
      if param.class != Hash
        raise ConfigError, "hash required but got #{val.inspect}"
      end
      if opts.empty?
        param
      else
        newparam = {}
        param.each_pair do |key, value|
          new_key = opts[:symbolize_keys] ? key.to_sym : key
          newparam[new_key] = opts[:value_type] ? REFORMAT_VALUE.call(opts[:value_type], value) : value
        end
        newparam
      end
    }
    ARRAY_TYPE = Proc.new { |val, opts|
      param = if val.is_a?(String)
                val.start_with?('[') ? JSON.load(val) : val.strip.split(/\s*,\s*/)
              else
                val
              end
      if param.class != Array
        raise ConfigError, "array required but got #{val.inspect}"
      end
      if opts[:value_type]
        param.map{|v| REFORMAT_VALUE.call(opts[:value_type], v) }
      else
        param
      end
    }
  end
end
