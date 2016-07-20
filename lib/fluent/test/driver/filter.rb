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

require 'fluent/test/driver/base_owner'
require 'fluent/test/driver/event_feeder'

require 'fluent/plugin/filter'

module Fluent
  module Test
    module Driver
      class Filter < BaseOwner
        include EventFeeder

        attr_reader :filtered

        def initialize(klass, opts: {}, &block)
          super
          raise ArgumentError, "plugin is not an instance of Fluent::Plugin::Filter" unless @instance.is_a? Fluent::Plugin::Filter
          @filtered = []
        end

        def filtered_records
          @filtered.map {|_time, record| record }
        end

        def instance_hook_after_started
          super
          filter_hook = ->(time, record) { @filtered << [time, record] }
          m = Module.new do
            define_method(:filter_stream) do |tag, es|
              new_es = super(tag, es)
              new_es.each do |time, record|
                filter_hook.call(time, record)
              end
              new_es
            end
          end
          @instance.singleton_class.prepend(m)
        end
      end
    end
  end
end
