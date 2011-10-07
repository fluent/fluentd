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
module Fluent
module Test
  autoload :TestDriver, 'fluent/test/base'
  autoload :OutputTestDriver, 'fluent/test/output_test'
  autoload :BufferedOutputTestDriver, 'fluent/test/output_test'

  @@test = false

  def test?
    @@test
  end

  def self.setup
    @@test = true

    require 'fluent/engine'
    require 'fluent/log'

    $log = Fluent::Log.new unless $log
    Fluent.__send__(:remove_const, :Engine)
    Fluent.const_set(:Engine, EngineClass.new).init
  end
end
end
