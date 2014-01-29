#
# Fluentd
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

require 'optparse'
require 'fluent/log'
require 'fluent/env'
require 'fluent/version'

$fluentdargv = Marshal.load(Marshal.dump(ARGV))

op = OptionParser.new
op.version = Fluent::VERSION

# default values
opts = {
  :config_path => Fluent::DEFAULT_CONFIG_PATH,
  :plugin_dirs => [Fluent::DEFAULT_PLUGIN_DIR],
  :log_level => Fluent::Log::LEVEL_INFO,
  :log_path => nil,
  :daemonize => false,
  :libs => [],
  :setup_path => nil,
  :chuser => nil,
  :chgroup => nil,
  :suppress_interval => 0,
  :usespawn => 0,
  :signame => nil
}

op.on('-s', "--setup [DIR=#{File.dirname(Fluent::DEFAULT_CONFIG_PATH)}]", "install sample configuration file to the directory") {|s|
  opts[:setup_path] = s || File.dirname(Fluent::DEFAULT_CONFIG_PATH)
}

op.on('-c', '--config PATH', "config file path (default: #{Fluent::DEFAULT_CONFIG_PATH})") {|s|
  opts[:config_path] = s
}

op.on('--dry-run', "Check fluentd setup is correct or not", TrueClass) {|b|
  opts[:dry_run] = b
}

op.on('-p', '--plugin DIR', "add plugin directory") {|s|
  opts[:plugin_dirs] << s
}

op.on('-I PATH', "add library path") {|s|
  $LOAD_PATH << s
}

op.on('-r NAME', "load library") {|s|
  opts[:libs] << s
}

op.on('-d', '--daemon PIDFILE', "daemonize fluent process") {|s|
  opts[:daemonize] = s
}

op.on('--user USER', "change user") {|s|
  opts[:chuser] = s
}

op.on('--group GROUP', "change group") {|s|
  opts[:chgroup] = s
}

op.on('-o', '--log PATH', "log file path") {|s|
  opts[:log_path] = s
}

op.on('-i', '--inline-config CONFIG_STRING', "inline config which is appended to the config file on-fly") {|s|
  opts[:inline_config] = s
}

op.on('--emit-error-log-interval SECONDS', "suppress interval seconds of emit error logs") {|s|
  opts[:suppress_interval] = s.to_i
}

op.on('-v', '--verbose', "increase verbose level (-v: debug, -vv: trace)", TrueClass) {|b|
  if b
    opts[:log_level] = [opts[:log_level] - 1, Fluent::Log::LEVEL_TRACE].max
  end
}

op.on('-q', '--quiet', "decrease verbose level (-q: warn, -qq: error)", TrueClass) {|b|
  if b
    opts[:log_level] = [opts[:log_level] + 1, Fluent::Log::LEVEL_ERROR].min
  end
}

op.on('-u', '--usespwan', "*** internal use only *** use spawn instead of fork (Windows only)", TrueClass) {|b|
  opts[:usespawn] = 1
}

op.on('-s', '--signame INTSIGNAME', "an object name which is used for Windows Service signal (Windows only)") {|s|
  opts[:signame] = s
}

(class<<self;self;end).module_eval do
  define_method(:usage) do |msg|
    puts op.to_s
    puts "error: #{msg}" if msg
    exit 1
  end
end

begin
  op.parse!(ARGV)

  if ARGV.length != 0
    usage nil
  end
rescue
  usage $!.to_s
end


if setup_path = opts[:setup_path]
  require 'fileutils'
  FileUtils.mkdir_p File.join(setup_path, "plugin")
  confpath = File.join(setup_path, "fluent.conf")
  if File.exist?(confpath)
    puts "#{confpath} already exists."
  else
    File.open(confpath, "w") {|f|
      conf = File.read File.join(File.dirname(__FILE__), "..", "..", "..", "fluent.conf")
      f.write conf
    }
    puts "Installed #{confpath}."
  end
  exit 0
end

require 'fluent/supervisor'
Fluent::Supervisor.new(opts).start

