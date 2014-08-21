module Fluent
  class RelabelOutput < Output
    Plugin.register_output('relabel', self)

    def emit(tag, es, chain)
      router.emit_stream(tag, es)
      chain.next
    end
  end
end
