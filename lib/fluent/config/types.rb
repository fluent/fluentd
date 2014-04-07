module Fluent
  Configurable.register_type(:string, Proc.new { |val, opts|
    val
  })

  Configurable.register_type(:integer, Proc.new { |val, opts|
    val.to_i
  })

  Configurable.register_type(:float, Proc.new { |val, opts|
    val.to_f
  })

  Configurable.register_type(:size, Proc.new { |val, opts|
    Config.size_value(val)
  })

  Configurable.register_type(:bool, Proc.new { |val, opts|
    Config.bool_value(val)
  })

  Configurable.register_type(:time, Proc.new { |val, opts|
    Config.time_value(val)
  })

  Configurable.register_type(:hash, Proc.new { |val, opts|
    param = JSON.load(val)
    if param.class != Hash
      raise ConfigError, "hash required but got #{val.inspect}"
    end
    param
  })

  Configurable.register_type(:array, Proc.new { |val, opts|
    param = JSON.load(val)
    if param.class != Array
      raise ConfigError, "array required but got #{val.inspect}"
    end
    param
  })
end
