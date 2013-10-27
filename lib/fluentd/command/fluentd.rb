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

LOG_LEVEL_TRACE = -1
LOG_LEVEL_INFO = 1
LOG_LEVEL_ERROR = 3

op = OptionParser.new

default_config_path = ENV['FLUENTD_CONFIG_PATH'] || '/etc/fluentd.conf'
default_plugin_dir = ENV['FLUENTD_PLUGIN_DIR'] || ['/etc/fluentd/plugin']
worker_process_name = ENV['FLUENTD_WORKER_PROCESS_NAME'] || 'fluentd:worker'

opts = {
  config_path: default_config_path,
  plugin_dirs: default_plugin_dir,
  load_path: [],
  log_level: LOG_LEVEL_INFO,
  log: nil,
  daemonize: false,
  libs: [],
  setup_path: nil,
  chuser: nil,
  chgroup: nil,
  worker_process_name: worker_process_name,
  suppress_config_dump: false,
}

op.on('-s', '--setup [DIR]', "install sample configuration file to the directory (defalut: #{default_config_path})") {|s|
  opts[:setup_path] = s || default_config_path
}

op.on('-c', '--config PATH', "config file path (default: #{default_config_path})") {|s|
  opts[:config_path] = s
}

op.on('-p', '--plugin DIR', "add plugin directory") {|s|
  opts[:plugin_dirs] << s
}

op.on('-I PATH', "add library path") {|s|
  opts[:load_path] << s
}

op.on('-r NAME', "load library") {|s|
  opts[:libs] << s
}

op.on('-g', '--gemfile GEMFILE', "Gemfile path") {|s|
  opts[:gemfile] = s
}

op.on('-G', '--gem-path GEM_INSTALL_PATH', "Gemfile install path (default: $(dirname $gemfile)/vendor/bundle)") {|s|
  opts[:gem_install_path] = s
}

op.on('--use-shared-gems', "Enable gems not installed into gem-path", TrueClass) {|b|
  opts[:use_shared_gems] = b
}

op.on('-d', '--daemon PIDFILE', "daemonize fluent process") {|s|
  opts[:daemonize] = true
  opts[:pid_path] = s
}

op.on('--user USER', "change user of worker processes") {|s|
  opts[:chuser] = s
}

op.on('--group GROUP', "change group of worker processes") {|s|
  opts[:chgroup] = s
}

op.on('-o', '--log PATH', "log file path") {|s|
  opts[:log] = s
}

# TODO
#op.on('-i', '--inline-config CONFIG_STRING', "inline config which is appended to the config file on-fly") {|s|
#  opts[:inline_config] = s
#}

op.on('--suppress-config-dump', "suppress config dumping when fluentd starts", TrueClass) {|b|
  opts[:suppress_config_dump] = b
}

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

op.on_tail("--version", "Show version") do
  require 'fluentd/version'
  puts "fluentd #{Fluentd::VERSION}"
  exit
end

begin
  rest = op.parse(ARGV)

  if rest.length != 0
    usage nil
  end
rescue => e
  usage e.to_s
end

##
## Bundler injection
#
if ENV['FLUENTD_DISABLE_BUNDLER_INJECTION'] != '1' && gemfile = opts[:gemfile]
  ENV['BUNDLE_GEMFILE'] = gemfile
  if path = opts[:gem_install_path]
    ENV['BUNDLE_PATH'] = path
  else
    ENV['BUNDLE_PATH'] = File.expand_path(File.join(File.dirname(gemfile), 'vendor/bundle'))
  end
  ENV['FLUENTD_DISABLE_BUNDLER_INJECTION'] = '1'
  load File.expand_path(File.join(File.dirname(__FILE__), 'bundler_injection.rb'))
end

##
## Setup configuration file and exit
#
if setup_path = opts[:setup_path]
  require 'fileutils'
  sample_conf = File.read File.join(File.dirname(__FILE__), "..", "..", "..", "fluentd.conf")

  conf_path = File.join(setup_path, File.basename(default_config_path))
  FileUtils.mkdir_p File.join(setup_path, "plugin")

  if File.exist?(conf_path)
    puts "#{conf_path} already exists."
    exit 1
  end

  File.open(conf_path, "w") {|f|
    f.write sample_conf
  }
  puts "Installed #{conf_path}."
  puts "Run following command to start:"
  puts ""
  puts "  $ #{$0} -c #{conf_path}"
  puts ""
  exit 0
end

##
## Start server
#
# add library root to the head of $LOAD_PATH to prioritize bundler
$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '../..'))
require 'fluentd/server'
Fluentd::Server.run(opts)

