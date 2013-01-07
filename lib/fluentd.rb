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
    :ConfigContext => 'fluentd/config_context',
    :Configurable => 'fluentd/configurable',
    :MatchPattern => 'fluentd/match_pattern',
    :MessageBus => 'fluentd/message_bus',
    :LabeledMessageBus => 'fluentd/message_bus',
    :OffsetMessageBus => 'fluentd/message_bus',
    :Writer => 'fluentd/writer',
    :MultiWriter => 'fluentd/multi_writer',
    :PluginRegistry => 'fluentd/plugin',
    :PluginClass => 'fluentd/plugin',
    :Outputs => 'fluentd/outputs',
    :Inputs => 'fluentd/inputs',
    :Chunks => 'fluentd/chunks',
    :Buffers => 'fluentd/buffers',
    :Writers => 'fluentd/writers',
    :StreamSource => 'fluentd/stream_source',
    :ProcessManager => 'fluentd/process_manager',
    :Processors => 'fluentd/processors',
    :Engine => 'fluentd/engine',
    :Server => 'fluentd/server',
    :Supervisor => 'fluentd/supervisor',
    :IOExchange => 'fluentd/io_exchange',
    :InternalUNIXServer => 'fluentd/internal_unix_server',
    :SocketManager => 'fluentd/socket_manager',
    :HeartbeatManager => 'fluentd/heartbeat_manager',
    :Logger => 'fluentd/logger',
    :BlockingFlag => 'fluentd/util/blocking_flag',
    :SignalQueue => 'fluentd/util/signal_queue',
  }.each_pair {|k,v|
    autoload k, File.join(here, v)
  }

  [
    'fluentd/version',
    'fluentd/env',
    'fluent',
  ].each {|v|
    require File.join(here, v)
  }
end

