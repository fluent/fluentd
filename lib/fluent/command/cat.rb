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

require 'optparse'
require 'fluent/env'
require 'fluent/time'
require 'fluent/msgpack_factory'
require 'fluent/version'

op = OptionParser.new

op.banner += " <tag>"
op.version = Fluent::VERSION

port = 24224
host = '127.0.0.1'
unix = false
socket_path = Fluent::DEFAULT_SOCKET_PATH

config_path = Fluent::DEFAULT_CONFIG_PATH
format = 'json'
message_key = 'message'
time_as_integer = false
retry_limit = 5

op.on('-p', '--port PORT', "fluent tcp port (default: #{port})", Integer) {|i|
  port = i
}

op.on('-h', '--host HOST', "fluent host (default: #{host})") {|s|
  host = s
}

op.on('-u', '--unix', "use unix socket instead of tcp", TrueClass) {|b|
  unix = b
}

op.on('-s', '--socket PATH', "unix socket path (default: #{socket_path})") {|s|
  socket_path = s
}

op.on('-f', '--format FORMAT', "input format (default: #{format})") {|s|
  format = s
}

op.on('--json', "same as: -f json", TrueClass) {|b|
  format = 'json'
}

op.on('--msgpack', "same as: -f msgpack", TrueClass) {|b|
  format = 'msgpack'
}

op.on('--none', "same as: -f none", TrueClass) {|b|
  format = 'none'
}

op.on('--message-key KEY', "key field for none format (default: #{message_key})") {|s|
  message_key = s
}

op.on('--time-as-integer', "Send time as integer for v0.12 or earlier", TrueClass) { |b|
  time_as_integer = true
}

op.on('--retry-limit N', "Specify the number of retry limit (default: #{retry_limit})", Integer) {|n|
  retry_limit = n
}

singleton_class.module_eval do
  define_method(:usage) do |msg|
    puts op.to_s
    puts "error: #{msg}" if msg
    exit 1
  end
end

begin
  op.parse!(ARGV)

  if ARGV.length != 1
    usage nil
  end

  tag = ARGV.shift

rescue
  usage $!.to_s
end


require 'thread'
require 'monitor'
require 'socket'
require 'yajl'
require 'msgpack'


class Writer
  include MonitorMixin

  RetryLimitError = Class.new(StandardError)

  class TimerThread
    def initialize(writer)
      @writer = writer
    end

    def start
      @finish = false
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @finish = true
      @thread.join
    end

    def run
      until @finish
        sleep 1
        @writer.on_timer
      end
    end
  end

  def initialize(tag, connector, time_as_integer: false, retry_limit: 5)
    @tag = tag
    @connector = connector
    @socket = false

    @socket_time = Time.now.to_i
    @socket_ttl = 10  # TODO
    @error_history = []

    @pending = []
    @pending_limit = 1024  # TODO
    @retry_wait = 1
    @retry_limit = retry_limit
    @time_as_integer = time_as_integer

    super()
  end

  def write(record)
    if record.class != Hash
      raise ArgumentError, "Input must be a map (got #{record.class})"
    end

    time = Fluent::EventTime.now
    time = time.to_i if @time_as_integer
    entry = [time, record]
    synchronize {
      unless write_impl([entry])
        # write failed
        @pending.push(entry)

        while @pending.size > @pending_limit
          # exceeds pending limit; trash oldest record
          time, record = @pending.shift
          abort_message(time, record)
        end
      end
    }
  end

  def on_timer
    now = Time.now.to_i

    synchronize {
      unless @pending.empty?
        # flush pending records
        if write_impl(@pending)
          # write succeeded
          @pending.clear
        end
      end

      if @socket && @socket_time + @socket_ttl < now
        # socket is not used @socket_ttl seconds
        close
      end
    }
  end

  def close
    @socket.close
    @socket = nil
  end

  def start
    @timer = TimerThread.new(self)
    @timer.start
    self
  end

  def shutdown
    @timer.shutdown
  end

  private
  def write_impl(array)
    socket = get_socket
    unless socket
      return false
    end

    begin
      packer = Fluent::MessagePackFactory.packer
      socket.write packer.pack([@tag, array])
      socket.flush
    rescue
      $stderr.puts "write failed: #{$!}"
      close
      return false
    end

    return true
  end

  def get_socket
    unless @socket
      unless try_connect
        return nil
      end
    end

    @socket_time = Time.now.to_i
    return @socket
  end

  def try_connect
    begin
      now = Time.now.to_i

      unless @error_history.empty?
        # wait before re-connecting
        wait = 1 #@retry_wait * (2 ** (@error_history.size-1))
        if now <= @socket_time + wait
          sleep(wait)
          try_connect
        end
      end

      @socket = @connector.call
      @error_history.clear
      return true

    rescue RetryLimitError => ex
      raise ex
    rescue
      $stderr.puts "connect failed: #{$!}"
      @error_history << $!
      @socket_time = now

      if @retry_limit < @error_history.size
        # abort all pending records
        @pending.each {|(time, record)|
          abort_message(time, record)
        }
        @pending.clear
        @error_history.clear
        raise RetryLimitError, "exceed retry limit"
      else
        retry
      end
    end
  end

  def abort_message(time, record)
    $stdout.puts "!#{time}:#{Yajl.dump(record)}"
  end
end


if unix
  connector = Proc.new {
    UNIXSocket.open(socket_path)
  }
else
  connector = Proc.new {
    TCPSocket.new(host, port)
  }
end

w = Writer.new(tag, connector, time_as_integer: time_as_integer, retry_limit: retry_limit)
w.start

case format
when 'json'
  begin
    while line = $stdin.gets
      record = Yajl.load(line)
      w.write(record)
    end
  rescue
    $stderr.puts $!
    exit 1
  end

when 'msgpack'
  require 'fluent/engine'

  begin
    u = Fluent::Engine.msgpack_factory.unpacker($stdin)
    u.each {|record|
      w.write(record)
    }
  rescue EOFError
  rescue
    $stderr.puts $!
    exit 1
  end

when 'none'
  begin
    while line = $stdin.gets
      record = { message_key => line.chomp }
      w.write(record)
    end
  rescue
    $stderr.puts $!
    exit 1
  end

else
  $stderr.puts "Unknown format '#{format}'"
  exit 1
end

