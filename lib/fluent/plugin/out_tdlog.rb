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

  HOST = ENV['TD_API_SERVER'] || 'api.treasure-data.com'
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
    @key = nil
  end

  def configure(conf)
    super

    @tmpdir = conf['tmpdir'] || @tmpdir
    FileUtils.mkdir_p(@tmpdir)

    @apikey = conf['apikey']
    unless @apikey
      raise ConfigError, "'apikey' parameter is required on tdlog output"
    end

    database = conf['database']
    table = conf['table']
    if database && table
      if !validate_name(database)
        raise ConfigError, "Invalid database name #{database.inspect}: #{conf}"
      end
      if !validate_name(table)
        raise ConfigError, "Invalid table name #{table.inspect}: #{conf}"
      end
      @key = "#{database}.#{table}"
    elsif (database && !table) || (!database && table)
      raise ConfigError, "'database' and 'table' parameter are required on tdlog output"
    end
  end

  def emit(tag, es, chain)
    # TODO check table name is in the list
    # TODO if not exists, get table list from API server
    # TODO if not exists, raise error
    if @key
      key = @key
    else
      database, table = tag.split('.')[-2,2]
      if !validate_name(database) || !validate_name(table)
        $log.debug { "Invalid tag #{tag.inspect}" }
        return
      end
      key = "#{database}.#{table}"
    end

    super(tag, es, chain, key)
  end

  def validate_name(name)
    # TODO
    true
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
    database, table = chunk.key.split('.',2)
    if !validate_name(database) || !validate_name(table)
      $log.error "Invalid key name #{chunk.key.inspect}"
      return
    end

    f = Tempfile.new("tdlog-", @tmpdir)
    w = Zlib::GzipWriter.new(f)

    chunk.write_to(w)
    w.finish
    w = nil

    size = f.pos
    f.pos = 0
    upload(database, table, f, size)

  ensure
    w.close if w
    f.close if f
  end

  def upload(database, table, io, size)
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
    header['Authorization'] = "TD1 #{@apikey}"
    header['Date'] = Time.now.rfc2822
    header['Content-Length'] = size.to_s
    header['Content-Type'] = 'application/octet-stream'

    url = "/v3/table/import/#{e database}/#{e table}/msgpack.gz"

    req = Net::HTTP::Put.new(url, header)
    if req.respond_to?(:body_stream=)
      req.body_stream = io
    else  # Ruby 1.8
      req.body = io.read
    end

    $log.trace { "uploading logs to TreasureData database=#{database} table=#{table} (#{size}bytes)" }

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
