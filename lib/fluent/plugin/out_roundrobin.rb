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

require 'fluent/plugin/multi_output'
require 'fluent/config/error'

module Fluent::Plugin
  class RoundRobinOutput < MultiOutput
    Fluent::Plugin.register_output('roundrobin', self)

    config_section :store do
      config_param :weight, :integer, default: 1
    end

    def initialize
      super
      @weights = []
    end

    attr_reader :weights

    def configure(conf)
      super

      @stores.each do |store|
        @weights << store.weight
      end
      @rr = -1  # starts from @output[0]
      @rand_seed = Random.new.seed
    end

    def multi_workers_ready?
      true
    end

    def start
      super
      rebuild_weight_array
    end

    def process(tag, es)
      next_output.emit_events(tag, es)
    end

    private

    def next_output
      @rr = 0 if (@rr += 1) >= @weight_array.size
      @weight_array[@rr]
    end

    def rebuild_weight_array
      gcd = @weights.inject(0) {|r,w| r.gcd(w) }

      weight_array = []
      @outputs.zip(@weights).each {|output,weight|
        (weight / gcd).times {
          weight_array << output
        }
      }

      # don't randomize order if all weight is 1 (=default)
      if @weights.any? {|w| w > 1 }
        r = Random.new(@rand_seed)
        weight_array.sort_by! { r.rand }
      end

      @weight_array = weight_array
    end
  end
end
