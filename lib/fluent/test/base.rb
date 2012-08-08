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


class TestDriver
  include ::Test::Unit::Assertions

  def initialize(klass, &block)
    if klass.is_a?(Class)
      if block
        klass = klass.dup
        klass.module_eval(&block)
      end
      @instance = klass.new
    else
      @instance = klass
    end
    @config = Config.new
  end

  attr_reader :instance, :config

  def configure(str)
    if str.is_a?(Config)
      @config = @str
    else
      @config = Config.parse(str, "(test)")
    end
    @instance.configure(@config)
    self
  end

  def run(&block)
    @instance.start
    begin
      # wait until thread starts
      10.times { sleep 0.05 }
      return yield
    ensure
      @instance.shutdown
    end
  end
end


end
end
