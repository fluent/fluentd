module Fluent::PluginSpecUser; end
using Fluent::PluginSpecUser

module Fluent
  class RetagOutput < Output
    Plugin.register_output('retag', self)

    config_param :tag, :string

    def emit(tag, time, record)
      @log.info('emit')
      Fluent::Engine.emit(@tag, time, record)
    end

    def emits(tag, es)
      @log.debug('emits')
      Fluent::Engine.emits(@tag, es)
    end
  end
end
