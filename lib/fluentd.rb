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
module Fluentd

  require 'fileutils'
  require 'tempfile'
  require 'tmpdir'
  require 'forwardable'

  require 'json'
  require 'yajl'

  require 'serverengine'

  here = File.expand_path(File.dirname(__FILE__))

  {
    :Collector => 'fluentd/collector',
    :Collectors => 'fluentd/collector',
    :Agent => 'fluentd/agent',
    :RootAgent => 'fluentd/root_agent',
    :MessageRouter => 'fluentd/message_router',
    :MessageSource => 'fluentd/message_source',
    :MatchPattern => 'fluentd/match_pattern',
    :Config => 'fluentd/config',
    :Configurable => 'fluentd/configurable',
    :StatsCollector => 'fluentd/stats_collector',
    :NullStatsCollector => 'fluentd/stats_collector',
    :Server => 'fluentd/server',
    :Worker => 'fluentd/worker',
    :Logger => 'fluentd/logger',
    :ProcessManager => 'fluentd/process_manager',
    :SocketManager => 'fluentd/socket_manager',
    :EventStream => 'fluentd/event_stream',
    :OneEventStream => 'fluentd/event_stream',
    :MultiEventStream => 'fluentd/event_stream',
    :MessagePackEventStream => 'fluentd/event_stream',
    :MultiProcessManager => 'fluentd/multi_process_manager',
    :InterProcessExchange => 'fluentd/multi_process_manager',
    :Plugin => 'fluentd/plugin',
    :PluginRegistry => 'fluentd/plugin',
    :Actor => 'fluentd/actor',
    :Actors => 'fluentd/actor',
    :ActorAgent => 'fluentd/actor',
    :BundlerInjection => 'fluentd/bundler_injection',
    :VERSION => 'fluentd/version',
  }.each_pair {|k,v|
    autoload k, File.join(here, v)
  }

  [
    'fluentd/process_global_methods',
  ].each {|v|
    require File.join(here, v)
  }

end
