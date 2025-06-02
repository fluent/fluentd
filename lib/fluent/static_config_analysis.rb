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

require 'fluent/config'
require 'fluent/plugin'

module Fluent
  # Static Analysis means analysing all plugins and Fluent::Element without invoking Plugin#configure
  class StaticConfigAnalysis
    module Elem
      Input = Struct.new(:plugin, :config)
      Output = Struct.new(:plugin, :config)
      Filter = Struct.new(:plugin, :config)
      Label = Struct.new(:name, :config, :nodes)
      Worker = Struct.new(:ids, :config, :nodes)
    end

    Result = Struct.new(:tree, :outputs, :inputs, :filters, :labels) do
      def all_plugins
        (outputs + inputs + filters).map(&:plugin)
      end
    end

    # @param workers [Integer] Number of workers
    # @return [Fluent::StaticConfigAnalysis::Result]
    def self.call(conf, workers: 1)
      new(workers).call(conf)
    end

    def initialize(workers)
      @workers = workers

      reset
    end

    def call(config)
      reset

      tree = [
        static_worker_analyse(config),
        static_label_analyse(config),
        static_filter_and_output_analyse(config),
        static_input_analyse(config),
      ].flatten

      Result.new(tree, @outputs, @inputs, @filters, @labels.values)
    end

    private

    def reset
      @outputs = []
      @inputs = []
      @filters = []
      @labels = {}
    end

    def static_worker_analyse(conf)
      available_worker_ids = [*0...@workers]

      ret = []
      supported_directives = %w[source match filter label]
      conf.elements(name: 'worker').each do |config|
        ids = parse_worker_id(config)
        ids.each do |id|
          if available_worker_ids.include?(id)
            available_worker_ids.delete(id)
          else
            raise Fluent::ConfigError, "specified worker_id<#{id}> collisions is detected on <worker> directive. Available worker id(s): #{available_worker_ids}"
          end
        end

        config.elements.each do |elem|
          unless supported_directives.include?(elem.name)
            raise Fluent::ConfigError, "<worker> section cannot have <#{elem.name}> directive"
          end
        end

        nodes = [
          static_label_analyse(config),
          static_filter_and_output_analyse(config),
          static_input_analyse(config),
        ].flatten
        ret << Elem::Worker.new(ids, config, nodes)
      end

      ret
    end

    def parse_worker_id(conf)
      worker_id_str = conf.arg

      if worker_id_str.empty?
        raise Fluent::ConfigError, 'Missing worker id on <worker> directive'
      end

      l, r =
         begin
           worker_id_str.split('-', 2).map { |v| Integer(v) }
         rescue TypeError, ArgumentError
           raise Fluent::ConfigError, "worker id should be integer: #{worker_id_str}"
         end

      if l < 0 || l >= @workers
        raise Fluent::ConfigError, "worker id #{l} specified by <worker> directive is not allowed. Available worker id is between 0 and #{@workers-1}"
      end

      # e.g. specified one worker id like `<worker 0>`
      if r.nil?
        return [l]
      end

      if r < 0 || r >= @workers
        raise Fluent::ConfigError, "worker id #{r} specified by <worker> directive is not allowed. Available worker id is between 0 and #{@workers-1}"
      end

      if l > r
        raise Fluent::ConfigError, "greater first_worker_id<#{l}> than last_worker_id<#{r}> specified by <worker> directive is not allowed. Available multi worker assign syntax is <smaller_worker_id>-<greater_worker_id>"
      end

      [l, r]
    end

    def static_label_analyse(conf)
      ret = []
      conf.elements(name: 'label').each do |e|
        name = e.arg
        if name.empty?
          raise ConfigError, 'Missing symbol argument on <label> directive'
        end

        if @labels[name]
          raise ConfigError, "Section <label #{name}> appears twice"
        end

        l = Elem::Label.new(name, e, static_filter_and_output_analyse(e))
        ret << l
        @labels[name] = l
      end

      ret
    end

    def static_filter_and_output_analyse(conf)
      ret = []
      conf.elements('filter', 'match').each do |e|
        type = e['@type']
        if type.nil? || type.empty?
          raise Fluent::ConfigError, "Missing '@type' parameter on <#{e.name}> directive"
        end

        if e.name == 'filter'
          f = Elem::Filter.new(Fluent::Plugin.new_filter(type), e)
          ret << f
          @filters << f
        else
          o = Elem::Output.new(Fluent::Plugin.new_output(type), e)
          ret << o
          @outputs << o
        end
      end

      ret
    end

    def static_input_analyse(conf)
      ret = []
      conf.elements(name: 'source').each do |e|
        type = e['@type']
        if type.nil? || type.empty?
          raise Fluent::ConfigError, "Missing '@type' parameter on <#{e.name}> directive"
        end

        i = Elem::Input.new(Fluent::Plugin.new_input(type), e)
        @inputs << i
        ret << i
      end

      ret
    end
  end
end
