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


class SyslogInput < Input
  Plugin.register_input('syslog', self)

  SYSLOG_REGEXP = /^\<([0-9]+)\>(.*)/
  SYSLOG_ALL_REGEXP = /^\<(?<pri>[0-9]+)\>(?<time>[^ ]* [^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[a-zA-Z0-9_\/\.\-]*)(?:\[(?<pid>[0-9]+)\])?[^\:]*\: *(?<message>.*)$/
  TIME_FORMAT = "%b %d %H:%M:%S"

  FACILITY_MAP = {
    0   => 'kern',
    1   => 'user',
    2   => 'mail',
    3   => 'daemon',
    4   => 'auth',
    5   => 'syslog',
    6   => 'lpr',
    7   => 'news',
    8   => 'uucp',
    9   => 'cron',
    10  => 'authpriv',
    11  => 'ftp',
    12  => 'ntp',
    13  => 'audit',
    14  => 'alert',
    15  => 'at',
    16  => 'local0',
    17  => 'local1',
    18  => 'local2',
    19  => 'local3',
    20  => 'local4',
    21  => 'local5',
    22  => 'local6',
    23  => 'local7'
  }

  PRIORITY_MAP = {
    0  => 'emerg',
    1  => 'alert',
    2  => 'crit',
    3  => 'err',
    4  => 'warn',
    5  => 'notice',
    6  => 'info',
    7  => 'debug'
  }

  def initialize
    super
  end

  config_param :port, :integer, :default => 5140
  config_param :bind, :string, :default => '0.0.0.0'
  config_param :tag, :string

  def configure(conf)
    super

    parser = TextParser.new
    if parser.configure(conf, false)
      parser.use_template('syslog')
      @parser = parser
    else
      @parser = nil
    end
  end

  def start
    if @parser
      callback = method(:receive_data_parser)
    else
      callback = method(:receive_data)
    end

    @loop = Coolio::Loop.new

    $log.debug "listening syslog socket on #{@bind}:#{@port}"
    @usock = UDPSocket.new
    @usock.bind(@bind, @port)

    @handler = UdpHandler.new(@usock, callback)
    @loop.attach(@handler)

    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @loop.watchers.each {|w| w.detach }
    @loop.stop
    @handler.close
    @thread.join
  end

  def run
    @loop.run
  rescue
    $log.error "unexpected error", :error=>$!.to_s
    $log.error_backtrace
  end

  protected
  def receive_data_parser(data)
    m = SYSLOG_REGEXP.match(data)
    unless m
      $log.debug "invalid syslog message: #{data.dump}"
      return
    end
    pri = m[1].to_i
    text = m[2]

    time, record = @parser.parse(text)
    unless time && record
      return
    end

    emit(pri, time, record)

  rescue
    $log.warn data.dump, :error=>$!.to_s
    $log.debug_backtrace
  end

  def receive_data(data)
    m = SYSLOG_ALL_REGEXP.match(data)
    unless m
      $log.debug "invalid syslog message", :data=>data
      return
    end

    pri = nil
    time = nil
    record = {}

    m.names.each {|name|
      if value = m[name]
        case name
        when "pri"
          pri = value.to_i
        when "time"
          time = Time.strptime(value, TIME_FORMAT).to_i
        else
          record[name] = value
        end
      end
    }

    time ||= Engine.now

    emit(pri, time, record)

  rescue
    $log.warn data.dump, :error=>$!.to_s
    $log.debug_backtrace
  end

  private
  def emit(pri, time, record)
    facility = FACILITY_MAP[pri >> 3]
    priority = PRIORITY_MAP[pri & 0b111]

    tag = "#{@tag}.#{facility}.#{priority}"

    Engine.emit(tag, time, record)
  end

  class UdpHandler < Coolio::IO
    def initialize(io, callback)
      super(io)
      @io = io
      @callback = callback
    end

    def on_readable
      msg, addr = @io.recvfrom_nonblock(1024)
      #host = addr[3]
      #port = addr[1]
      #@callback.call(host, port, msg)
      @callback.call(msg)
    rescue
      # TODO log?
    end
  end
end


end
