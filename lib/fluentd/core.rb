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

  here = File.expand_path(File.dirname(__FILE__))

  {
    :Agent => 'core/agent',
    :AgentGroup => 'core/agent_group',
    :Config => 'core/config',
    :ConfigContext => 'core/config_context',
    :Configurable => 'core/configurable',
    :Engine => 'core/engine',
    :MatchPattern => 'core/match_pattern',
    :MessageBus => 'core/message_bus',
    :LabeledMessageBus => 'core/message_bus',
    :OffsetMessageBus => 'core/message_bus',
    :PluginRegistry => 'core/plugin_registry',
    :PluginClass => 'core/plugin_registry',
    :StreamSource => 'core/stream_source',
    :Server => 'core/server',
    :Supervisor => 'core/supervisor',
    :ProcessManager => 'core/process_manager',
    :Logger => 'core/logger',
    :BlockingFlag => 'core/util/blocking_flag',
    :SignalQueue => 'core/util/signal_queue',
  }.each_pair {|k,v|
    autoload k, File.join(here, v)
  }

  [
    'core/env',
  ].each {|v|
    require File.join(here, v)
  }

end
