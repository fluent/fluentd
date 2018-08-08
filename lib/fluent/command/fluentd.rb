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

require 'fluent/supervisor'
require 'fluent/log'
require 'fluent/env'
require 'fluent/version'

$fluentdargv = Marshal.load(Marshal.dump(ARGV))

op = OptionParser.new
op.version = Fluent::VERSION

opts = Fluent::Supervisor.default_options

op.on('-s', "--setup [DIR=#{File.dirname(Fluent::DEFAULT_CONFIG_PATH)}]", "install sample configuration file to the directory") {|s|
  opts[:setup_path] = s || File.dirname(Fluent::DEFAULT_CONFIG_PATH)
}

op.on('-c', '--config PATH', "config file path (default: #{Fluent::DEFAULT_CONFIG_PATH})") {|s|
  opts[:config_path] = s
}

op.on('--dry-run', "Check fluentd setup is correct or not", TrueClass) {|b|
  opts[:dry_run] = b
}

op.on('--show-plugin-config=PLUGIN', "Show PLUGIN configuration and exit(ex: input:dummy)") {|plugin|
  opts[:show_plugin_config] = plugin
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

op.on('--under-supervisor', "run fluent worker under supervisor (this option is NOT for users)") {
  opts[:supervise] = false
}

op.on('--no-supervisor', "run fluent worker without supervisor") {
  opts[:supervise] = false
  opts[:standalone_worker] = true
}

op.on('--workers NUM', "specify the number of workers under supervisor") { |i|
  opts[:workers] = i.to_i
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

ROTATE_AGE = %w(daily weekly monthly)
op.on('--log-rotate-age AGE', 'generations to keep rotated log files') {|age|
  if ROTATE_AGE.include?(age)
    opts[:log_rotate_age] = age
  else
    begin
      opts[:log_rotate_age] = Integer(age)
    rescue TypeError
      usage "log-rotate-age should be #{rotate_ages.join(', ')} or a number"
    end
  end
}

op.on('--log-rotate-size BYTES', 'sets the byte size to rotate log files') {|s|
  opts[:log_rotate_size] = s.to_i
}

op.on('--log-event-verbose', 'enable log events during process startup/shutdown') {|b|
  opts[:log_event_verbose] = b
}

op.on('-i', '--inline-config CONFIG_STRING', "inline config which is appended to the config file on-the-fly") {|s|
  opts[:inline_config] = s
}

op.on('--emit-error-log-interval SECONDS', "suppress interval seconds of emit error logs") {|s|
  opts[:suppress_interval] = s.to_i
}

op.on('--suppress-repeated-stacktrace [VALUE]', "suppress repeated stacktrace", TrueClass) {|b|
  b = true if b.nil?
  opts[:suppress_repeated_stacktrace] = b
}

op.on('--without-source', "invoke a fluentd without input plugins", TrueClass) {|b|
  opts[:without_source] = b
}

op.on('--use-v1-config', "Use v1 configuration format (default)", TrueClass) {|b|
  opts[:use_v1_config] = b
}

op.on('--use-v0-config', "Use v0 configuration format", TrueClass) {|b|
  opts[:use_v1_config] = !b
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

op.on('--suppress-config-dump', "suppress config dumping when fluentd starts", TrueClass) {|b|
  opts[:suppress_config_dump] = b
}

op.on('-g', '--gemfile GEMFILE', "Gemfile path") {|s|
  opts[:gemfile] = s
}

op.on('-G', '--gem-path GEM_INSTALL_PATH', "Gemfile install path (default: $(dirname $gemfile)/vendor/bundle)") {|s|
  opts[:gem_install_path] = s
}

if Fluent.windows?
  require 'windows/library'
  include Windows::Library
  
  opts.merge!(
    :winsvc_name => 'fluentdwinsvc',
    :winsvc_display_name => 'Fluentd Windows Service',
    :winsvc_desc => 'Fluentd is an event collector system.',
  )

  op.on('-x', '--signame INTSIGNAME', "an object name which is used for Windows Service signal (Windows only)") {|s|
    opts[:signame] = s
  }

  op.on('--reg-winsvc MODE', "install/uninstall as Windows Service. (i: install, u: uninstall) (Windows only)") {|s|
    opts[:regwinsvc] = s
  }

  op.on('--[no-]reg-winsvc-auto-start', "Automatically start the Windows Service at boot. (only effective with '--reg-winsvc i') (Windows only)") {|s|
    opts[:regwinsvcautostart] = s
  }

  op.on('--reg-winsvc-fluentdopt OPTION', "specify fluentd option parameters for Windows Service. (Windows only)") {|s|
    opts[:fluentdopt] = s
  }
  
  op.on('--winsvc-name NAME', "The Windows Service name to run as (Windows only)") {|s|
    opts[:winsvc_name] = s
  }
  
  op.on('--winsvc-display-name DISPLAY_NAME', "The Windows Service display name (Windows only)") {|s|
    opts[:winsvc_display_name] = s
  }
  
  op.on('--winsvc-desc DESC', "The Windows Service description (Windows only)") {|s|
    opts[:winsvc_desc] = s
  }
end


singleton_class.module_eval do
  define_method(:usage) do |msg|
    puts op.to_s
    puts "error: #{msg}" if msg
    exit 1
  end
end

begin
  rest = op.parse(ARGV)

  if rest.length != 0
    usage nil
  end
rescue
  usage $!.to_s
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

early_exit = false
start_service = false
if winsvcinstmode = opts[:regwinsvc]
  require 'fileutils'
  require "win32/service"
  require "win32/registry"
  include Win32

  case winsvcinstmode
  when 'i'
    binary_path = File.join(File.dirname(__FILE__), "..")
    ruby_path = "\0" * 256
    GetModuleFileName.call(0,ruby_path,256)
    ruby_path = ruby_path.rstrip.gsub(/\\/, '/')
    start_type = Service::DEMAND_START
    if opts[:regwinsvcautostart]
      start_type = Service::AUTO_START
      start_service = true
    end

    
    Service.create(
      service_name: opts[:winsvc_name],
      host: nil,
      service_type: Service::WIN32_OWN_PROCESS,
      description: opts[:winsvc_desc],
      start_type: start_type,
      error_control: Service::ERROR_NORMAL,
      binary_path_name: "\"#{ruby_path}\" -C \"#{binary_path}\"  winsvc.rb --service-name #{opts[:winsvc_name]}",
      load_order_group: "",
      dependencies: [""],
      display_name: opts[:winsvc_display_name]
    )
  when 'u'
    if Service.status(opts[:winsvc_name]).current_state != 'stopped'
      begin
        Service.stop(opts[:winsvc_name])
      rescue => ex
        puts "Warning: Failed to stop service: ", ex
      end
    end
    Service.delete(opts[:winsvc_name])
  else
    # none
  end
  early_exit = true
end

if fluentdopt = opts[:fluentdopt]
  Win32::Registry::HKEY_LOCAL_MACHINE.open("SYSTEM\\CurrentControlSet\\Services\\#{opts[:winsvc_name]}", Win32::Registry::KEY_ALL_ACCESS) do |reg|
    reg['fluentdopt', Win32::Registry::REG_SZ] = fluentdopt
  end
  early_exit = true
end

if start_service
  Service.start(opts[:winsvc_name])
end

exit 0 if early_exit

require 'fluent/supervisor'
if opts[:supervise]
  Fluent::Supervisor.new(opts).run_supervisor
else
  if opts[:standalone_worker] && opts[:workers] && opts[:workers] > 1
    puts "Error: multi workers is not supported with --no-supervisor"
    exit 2
  end
  Fluent::Supervisor.new(opts).run_worker
end
