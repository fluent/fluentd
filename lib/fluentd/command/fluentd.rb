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

require 'optparse'
require 'fluentd/version'

LOG_LEVEL_TRACE = -1
LOG_LEVEL_INFO = 1
LOG_LEVEL_ERROR = 3

op = OptionParser.new
op.version = Fluentd::VERSION

default_config_path = ENV['FLUENTD_CONFIG_PATH'] || '/etc/fluentd.conf'
default_plugin_dir = ENV['FLUENTD_PLUGIN_DIR'] || ['/etc/fluentd/plugin']

opts = {
  :config_path => default_config_path,
  :plugin_dirs => default_plugin_dir,
  :log_level => LOG_LEVEL_INFO,
  :log => nil,
  :daemonize => false,
  :libs => [],
  :setup_path => nil,
  :chuser => nil,
  :chgroup => nil,
}

op.on('-s', "--setup [DIR=#{default_config_path}]", "install sample configuration file to the directory") {|s|
  opts[:setup_path] = s || default_config_path
}

op.on('-c', '--config PATH', "config file path (default: #{default_config_path})") {|s|
  opts[:config_path] = s
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

op.on('-g', '--gemfile GEMFILE', "Gemfile path") {|s|
  opts[:gemfile] = s
}

op.on('-G', '--gem-path GEM_INSTALL_PATH', "Gemfile install path") {|s|
  opts[:gem_install_path] = s
}

op.on('--use-shared-gems', "Gemfile path", TrueClass) {|b|
  opts[:use_shared_gems] = b
}

op.on('-d', '--daemon PIDFILE', "daemonize fluent process") {|s|
  opts[:daemonize] = true
  opts[:pid_path] = s
}

op.on('--user USER', "change user") {|s|
  opts[:chuser] = s
}

op.on('--group GROUP', "change group") {|s|
  opts[:chgroup] = s
}

op.on('-o', '--log PATH', "log file path") {|s|
  opts[:log] = s
}

# TODO
#op.on('-i', '--inline-config CONFIG_STRING', "inline config which is appended to the config file on-fly") {|s|
#  opts[:inline_config] = s
#}

op.on('-v', '--verbose', "increase verbose level (-v: debug, -vv: trace)", TrueClass) {|b|
  if b
    opts[:log_level] = [opts[:log_level] - 1, LOG_LEVEL_TRACE].max
  end
}

op.on('-q', '--quiet', "decrease verbose level (-q: warn, -qq: error)", TrueClass) {|b|
  if b
    opts[:log_level] = [opts[:log_level] + 1, LOG_LEVEL_ERROR].min
  end
}

op.on('-D', '--parameter KEY=VALUE', 'other parameters') {|s|
  k, v = s.spilt('=',2)
  v ||= true
  opts[k.to_sym] = v
}

define_singleton_method(:usage) do |msg|
  puts op.to_s
  puts "error: #{msg}" if msg
  exit 1
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
  conf_path = File.join(setup_path, "fluentd.conf")
  if File.exist?(conf_path)
    puts "#{conf_path} already exists."
  else
    File.open(conf_path, "w") {|f|
      conf = File.read File.join(File.dirname(__FILE__), "..", "..", "..", "fluentd.conf")
      f.write conf
    }
    puts "Installed #{conf_path}."
  end
  exit 0
end

if gemfile = opts[:gemfile]
  require 'fluentd/bundler_injection'
  Fluentd::BundlerInjection.install(gemfile, opts)
end

require 'fluentd'
Fluentd::Server.run(opts)

