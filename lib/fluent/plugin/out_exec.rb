#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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


class ExecOutput < TimeSlicedOutput
  Plugin.register_output('exec', self)

  def initialize
    super
    require 'tempfile'
    @localtime = false
  end

  config_param :command, :string
  config_param :keys, :string
  config_param :tag_key, :string, :default => nil
  config_param :time_key, :string, :default => nil
  config_param :time_format, :string, :default => nil

  def configure(conf)
    super

    @keys = @keys.split(',')

    if @time_key
      if @time_format
        tf = TimeFormatter.new(@time_format, @localtime)
        @time_format_proc = tf.method(:format)
      else
        @time_format_proc = Proc.new {|time| time.to_s }
      end
    end
  end

  def format(tag, time, record)
    out = ''
    last = @keys.length-1
    for i in 0..last
      key = @keys[i]
      if key == @time_key
        out << @time_format_proc.call(time)
      elsif key == @tag_key
        out << tag
      else
        out << record[key].to_s
      end
      out << "\t" if i != last
    end
    out << "\n"
    out
  end

  def write(chunk)
    if chunk.respond_to?(:path)
      prog = "#{@command} #{chunk.path}"
    else
      tmpfile = Tempfile.new("fluent-plugin-exec-")
      chunk.write_to(tmpfile)
      tmpfile.close
      prog = "#{@command} #{tmpfile.path}"
    end

    system(prog)
    ecode = $?.to_i
    tmpfile.delete if tmpfile

    if ecode != 0
      raise "command returns #{ecode}: #{prog}"
    end
  end
end


end

