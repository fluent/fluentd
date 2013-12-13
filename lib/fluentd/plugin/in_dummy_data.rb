require_relative './input'

module Fluentd
  module Plugin
    class DummyData < Input
      Plugin.register_input('dummy_data', self)

      config_param :interval, :time, :default => 5
      config_param :tag, :string, :default => 'dummy'
      config_param :data, :hash

      def configure(conf)
        super
      end

      def start
        @running = true
        actor.every(@interval) do
          next unless @running
          event_router.emit(@tag, Time.now.to_i, @data)
        end

        super
      end

      def stop
        @running = false
        super
      end
    end
  end
end
