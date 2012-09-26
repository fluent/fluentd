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

require 'fluent/load'

$log = Fluent::Log.new(STDERR, Fluent::Log::LEVEL_TRACE)
Fluent::Engine.init

remote_engine = DRb::DRbObject.new_with_uri(uri)

Fluent.module_eval do
  remove_const(:Engine)
  const_set(:Engine, remote_engine)
end

include Fluent

puts "Connected to #{uri}."
puts "Usage:"
puts "    Engine.match('some.tag').output  : get an output plugin instance"
puts "    Engine.sources[i]                : get input plugin instances"
puts "    Plugin.load_plugin(type,name)    : load plugin class (use this if you get DRb::DRbUnknown)"
puts ""

Encoding.default_internal = nil if Encoding.respond_to?(:default_internal)

require 'irb'
IRB.start

