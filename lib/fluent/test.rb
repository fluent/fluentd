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

require 'test/unit'
require 'fluent/env' # for Fluent.windows?
require 'fluent/test/log'
require 'fluent/test/base'
require 'fluent/test/input_test'
require 'fluent/test/output_test'
require 'fluent/test/filter_test'
require 'fluent/test/parser_test'
require 'fluent/test/formatter_test'
require 'serverengine'


module Fluent
  module Test
    def self.dummy_logger
      dl_opts = {log_level: ServerEngine::DaemonLogger::INFO}
      logdev = Fluent::Test::DummyLogDevice.new
      logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
      Fluent::Log.new(logger)
    end

    def self.setup
      ENV['SERVERENGINE_WORKER_ID'] = '0'

      $log = dummy_logger

      Fluent.__send__(:remove_const, :Engine)
      engine = Fluent.const_set(:Engine, EngineClass.new).init(SystemConfig.new)

      engine.define_singleton_method(:now=) {|n|
        @now = n
      }
      engine.define_singleton_method(:now) {
        @now ||= super()
      }

      nil
    end
  end
end

$log ||= Fluent::Test.dummy_logger
