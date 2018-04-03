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

op = OptionParser.new

host = '127.0.0.1'
port = 24230
unix = nil

op.on('-h', '--host HOST', "fluent host (default: #{host})") {|s|
  host = s
}

op.on('-p', '--port PORT', "debug_agent tcp port (default: #{port})", Integer) {|i|
  port = i
}

op.on('-u', '--unix PATH', "use unix socket instead of tcp") {|b|
  unix = b
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

  if ARGV.length != 0
    usage nil
  end
rescue
  usage $!.to_s
end

require 'drb/drb'

if unix
  uri = "drbunix:#{unix}"
else
  uri = "druby://#{host}:#{port}"
end

require 'fluent/log'
require 'fluent/engine'
require 'fluent/system_config'
require 'serverengine'

include Fluent::SystemConfig::Mixin

dl_opts = {}
dl_opts[:log_level] = ServerEngine::DaemonLogger::TRACE
logger = ServerEngine::DaemonLogger.new(STDERR, dl_opts)
$log = Fluent::Log.new(logger)
Fluent::Engine.init(system_config)

DRb::DRbObject.class_eval do
  undef_method :methods
  undef_method :instance_eval
  undef_method :instance_variables
  undef_method :instance_variable_get
end

remote_engine = DRb::DRbObject.new_with_uri(uri)

Fluent.module_eval do
  remove_const(:Engine)
  const_set(:Engine, remote_engine)
end

include Fluent

puts "Connected to #{uri}."
puts "Usage:"
puts "    Fluent::Engine.root_agent.event_router.match('some.tag') : get an output plugin instance"
puts "    Fluent::Engine.root_agent.inputs[i]                      : get input plugin instances"
puts "    Fluent::Plugin::OUTPUT_REGISTRY.lookup(name)             : load output plugin class (use this if you get DRb::DRbUnknown)"
puts ""

Encoding.default_internal = nil if Encoding.respond_to?(:default_internal)

require 'irb'
IRB.start
