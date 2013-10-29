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

  Configurable.register_type(:object, lambda {|val,opts|
    val
  })

  Configurable.register_type(:string, lambda {|val,opts|
    val.to_s
  })

  Configurable.register_type(:integer, lambda {|val,opts|
    val = val.to_s
    if val =~ /\A\-?(?:0|[1-9][0-9]*)\z/
      val.to_i
    else
      raise ConfigError, "Expected integer but got #{val.dump}"
    end
  })

  Configurable.register_type(:float, lambda {|val,opts|
    val.to_s
    if val =~ /\A\-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\z/
      val.to_f
    else
      raise ConfigError, "Expected float but got #{val.dump}"
    end
  })

  Configurable.register_type(:size, lambda {|val, opts|
    val = val.to_s
    {
      'k' => 1024,
      'm' => 1024**2,
      'g' => 1024**3
      't' => 1024**4,
    }.each_pair {|k,v|
      if val =~ /\A(0|[1-9][0-9]*)[ \t]*#{k}\z/
        return m[1].to_i * v
      end
    }
    if val =~ /\A\-?(?:0|[1-9][0-9]*)\z/
      return val.to_i
    end
    raise ConfigError, "Expected size (integer + k, m, g) but got #{val.dump}"
  })

  Configurable.register_type(:boolean, lambda {|val,opts|
    if val.nil?
      return true
    end
    val = val.to_s
    case val
    when 'true'
      true
    when 'false'
      false
    else
      raise ConfigError, "Expected 'true' or 'false' but got #{val.dump}"
    end
  })

  Configurable.register_type(:time, lambda {|val,opts|
    f = case val.to_s
        when /([0-9]+)s/
          $~[1].to_f
        when /([0-9]+)m/
          $~[1].to_f * 60
        when /([0-9]+)h/
          $~[1].to_f * 60*60
        when /([0-9]+)d/
          $~[1].to_f * 24*60*60
        else
          val.to_f
        end
    f
  })

  Configurable.register_type(:hash, lambda {|val,opts|
    case val
    when Hash
      val
    when String
      JSON.load(val)
    else
      raise ConfigError, "hash required but got #{val.inspect}"
    end
  })

end
