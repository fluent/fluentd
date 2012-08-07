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
module Fluentd
  require 'json'
  require 'msgpack'
  require 'thread'
  require 'singleton'

  here = File.expand_path(File.dirname(__FILE__))

  {
    :Agent => 'fluentd/agent',
    :AgentGroup => 'fluentd/agent_group',
    :Collector => 'fluentd/collector',
    :Collectors => 'fluentd/collectors',
    :ConfigError => 'fluentd/errors',
    :ConfigParseError => 'fluentd/errors',
    :Config => 'fluentd/config',
    :MatchPattern => 'fluentd/match_pattern',
    :MessageBus => 'fluentd/message_bus',
    :LabeledMessageBus => 'fluentd/message_bus',
    :PluginRegistry => 'fluentd/plugin',
    :PluginClass => 'fluentd/plugin',
    :Plugin => 'fluentd/plugin',
    :Outputs => 'fluentd/outputs',
    :Inputs => 'fluentd/inputs',
    :Chunks => 'fluentd/chunks',
    :Buffers => 'fluentd/buffers',
    :StreamSource => 'fluentd/stream_source',
    :ProcessManager => 'fluentd/process_manager',
    :Processor => 'fluentd/process_manager',
    :Supervisor => 'fluentd/supervisor',
    :BlockingFlag => 'fluentd/util/blocking_flag',
    :DaemonsLogger => 'fluentd/util/daemons_logger',
    :SignalQueue => 'fluentd/util/signal_queue',
  }.each_pair {|k,v|
    autoload k, File.join(here, v)
  }

  [
    'fluentd/version',
  ].each {|v|
    require File.join(here, v)
  }
end

