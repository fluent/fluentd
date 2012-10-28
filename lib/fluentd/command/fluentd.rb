#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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
require 'fluentd'

op = OptionParser.new
op.version = Fluentd::VERSION

# default values
opts = {
  :config_path => Fluentd::DEFAULT_CONFIG_PATH,
  :plugin_dirs => [Fluentd::DEFAULT_PLUGIN_DIR],
  :log_level => Fluentd::Logger::LEVEL_INFO,
  :log_path => nil,
  :daemonize => false,
  :libs => [],
  #:setup_path => nil,
  :chuser => nil,
  :chgroup => nil,
  :config_check => false,
  :disable_supervisor => false,
}

#op.on('-s', "--setup [DIR=#{File.dirname(Fluentd::DEFAULT_CONFIG_PATH)}]", "install sample configuration file to the directory") {|s|
#  opts[:setup_path] = s || File.dirname(Fluentd::DEFAULT_CONFIG_PATH)
#}

op.on('-c', '--config PATH', "config file path (default: #{Fluentd::DEFAULT_CONFIG_PATH})") {|s|
  opts[:config_path] = s
}

op.on('--check', "check configuration file and exit", TrueClass) {|b|
  opts[:config_check] = b
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

op.on('--disable-supervisor', "disable supervisor which automatically restarts dead server", TrueClass) {|b|
  opts[:disable_supervisor] = b
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

op.on('-v', '--verbose', "increase verbose level (-v: debug, -vv: trace)", TrueClass) {|b|
  if b
    opts[:log_level] = [opts[:log_level] - 1, Fluentd::Log::LEVEL_TRACE].max
  end
}

op.on('-q', '--quiet', "decrease verbose level (-q: warn, -qq: error)", TrueClass) {|b|
  if b
    opts[:log_level] = [opts[:log_level] + 1, Fluentd::Log::LEVEL_ERROR].min
  end
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

#if setup_path = opts[:setup_path]
#  require 'fileutils'
#  FileUtils.mkdir_p File.join(setup_path, "plugin")
#  confpath = File.join(setup_path, "fluent.conf")
#  if File.exist?(confpath)
#    puts "#{confpath} already exists."
#  else
#    File.open(confpath, "w") {|f|
#      conf = File.read File.join(File.dirname(__FILE__), "..", "..", "..", "fluent.conf")
#      f.write conf
#    }
#    puts "Installed #{confpath}."
#  end
#  exit 0
#end

config_load_proc = lambda {
  Fluentd::Config.read(opts[:config_path])
}

if opts[:config_check]
  sv = Fluentd::Server.new(&config_load_proc)
  sv.setup!
  puts "ok."
  exit 0
end

if opts[:disable_supervisor]
  sv = Fluentd::Server.new(&config_load_proc)
else
  sv = Fluentd::Supervisor.new(&config_load_proc)
end

#if opts[:daemonize]
#  sv.setup!
#  daemonize
#end

sv.run

