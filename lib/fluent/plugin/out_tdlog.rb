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


class TreasureDataLogOutput < BufferedOutput
  Plugin.register_output('tdlog', self)

  HOST = 'api.treasure-data.com'
  PORT = 80
  USE_SSL = false

  def initialize
    require 'fileutils'
    require 'tempfile'
    require 'zlib'
    require 'net/http'
    require 'cgi' # CGI.escape
    require 'time' # Time#rfc2822
    super
    @tmpdir = '/tmp/fluent/tdlog'
    @apikey = nil
  end

  def configure(conf)
    super

    @tmpdir = conf['tmpdir'] || @tmpdir
    FileUtils.mkdir_p(@tmpdir)

    @apikey = conf['apikey']
    unless @apikey
      raise ConfigError, "'apikey' parameter is required on td output"
    end

    @database = conf['database']
    unless @database
      raise ConfigError, "'database' parameter is required on td output"
    end

    @table = conf['table']
    unless @table
      raise ConfigError, "'table' parameter is required on td output"
    end
  end

  def format_stream(tag, es)
    out = ''
    es.each {|event|
      record = event.record
      record['time'] = event.time
      record.to_msgpack(out)
    }
    out
  end

  def write(chunk)
    f = Tempfile.new("tdlog-", @tmpdir)
    w = Zlib::GzipWriter.new(f)

    chunk.write_to(w)
    w.finish
    w = nil

    size = f.pos
    f.pos = 0
    upload(f, size)

  ensure
    w.close if w
    f.close if f
  end

  def upload(f, size)
    http = Net::HTTP.new(HOST, PORT)
    if USE_SSL
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_PEER
      store = OpenSSL::X509::Store.new
      http.cert_store = store
    end

    # TODO read_timeout
    #http.read_timeout = options[:read_timeout]

    header = {}
    header['Authorization'] = "TRD #{@apikey}"
    header['Date'] = Time.now.rfc2822
    header['Content-Length'] = size.to_s
    header['Content-Type'] = 'application/octet-stream'

    url = "/v3/table/import/#{e @database}/#{e @table}/msgpack.gz"

    req = Net::HTTP::Put.new(url, header)
    if req.respond_to?(:body_stream=)
      req.body_stream = f
    else  # Ruby 1.8
      req.body = f.read
    end

    $log.trace { "uploading logs to TreasureData table=#{@database}.#{@table} (#{size}bytes)" }

    response = http.request(req)

    if response.code[0] != ?2
      raise "TreasureData upload failed: #{response.body}"
    end
  end

  def e(s)
    CGI.escape(s.to_s)
  end
end


end

