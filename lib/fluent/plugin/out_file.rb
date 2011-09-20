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


class FileOutput < TimeSlicedOutput
  Plugin.register_output('file2', self)

  SUPPORTED_COMPRESS = {
    :gz => :gz,
    :gzip => :gz,
  }

  def initialize
    require 'zlib'
    super
    @path = nil
    @compress = nil
  end

  attr_accessor :path, :compress

  def configure(conf)
    if path = conf['path']
      @path = path
    end
    unless @path
      raise ConfigError, "'path' parameter is required on file output"
    end

    if pos = @path.index('*')
      @path_prefix = @path[0,pos]
      @path_suffix = @path[pos+1..-1]
      conf['buffer_path'] ||= "#{@path}"
    else
      @path_prefix = @path+"."
      @path_suffix = ".log"
      conf['buffer_path'] ||= "#{@path}.*"
    end

    super

    if compress = conf['compress']
      c = SUPPORTED_COMPRESS[compress.to_sym]
      unless c
        raise ConfigError, "Unsupported compression algorithm '#{compress}'"
      end
      @compress = c
    end
  end

  def format(tag, event)
    # TODO foramt
    #msg = e.record.map {|k,v|
    #	k = "\"#{k}\"" if k.include?(' ')
    #	v = "\"#{v}\"" if v.include?(' ')
    #	"#{k}=#{v}"
    #}.join(' ')
    msg = event.record.to_json
    "#{Time.at(event.time).to_s} #{tag}: #{msg}\n"
  end

  def write(chunk)
    i = 0
    begin
      path = "#{@path_prefix}#{chunk.key}_#{i}#{@path_suffix}"
      i += 1
    end while File.exist?(path)
    FileUtils.mkdir_p File.dirname(path)

    case @compress
    when nil
      File.open(path, "a") {|f|
        chunk.write_to(f)
      }
    when :gz
      Zlib::GzipWriter.open("#{path}.gz") {|f|
        chunk.write_to(f)
      }
    end
  end
end


end

