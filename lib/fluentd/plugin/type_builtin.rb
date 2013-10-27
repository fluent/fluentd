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

  Plugin.register_type(:any) do |val,opts|
    val
  end

  Plugin.register_type(:string) do |val,opts|
    val.to_s
  end

  Plugin.register_type(:integer) do |val,opts|
    val.to_i
  end

  Plugin.register_type(:float) do |val,opts|
    val.to_f
  end

  Plugin.register_type(:size) do |val, opts|
    case val.to_s
    when /([0-9]+)k/i
      $~[1].to_i * 1024
    when /([0-9]+)m/i
      $~[1].to_i * (1024**2)
    when /([0-9]+)g/i
      $~[1].to_i * (1024**3)
    when /([0-9]+)t/i
      $~[1].to_i * (1024**4)
    else
      val.to_i
    end
  end

  Plugin.register_type(:bool) do |val,opts|
    case val.to_s
    when 'true', 'yes', nil
      true
    when 'false', 'no'
      false
    else
      nil
    end
  end

  Plugin.register_type(:time) do |val,opts|
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
  end

  Plugin.register_type(:hash) do |val,opts|
    case val
    when Hash
      val
    when String
      JSON.load(val)
    else
      raise ConfigError, "hash required but got #{val.inspect}"
    end
  end

end
