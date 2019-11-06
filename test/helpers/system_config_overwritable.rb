require 'fluent/config'
require 'fluent/system_config'

module SystemConfigOverWritable
  def initialize
    super
    @overwrite_system_config = nil
  end

  def overwrite_system_config(hash)
    @overwrite_system_config = Fluent::SystemConfig.new(Fluent::Config::Element.new('system', '', hash, []))
    yield
  ensure
    @overwrite_system_config = nil
  end

  def system_config_override(opts = {})
    sc = system_config
    opts.each do |key, value|
      sc.__send__(:"#{key}=", value)
    end

    sc
  end

  def system_config
    sc = (@overwrite_system_config || super)
    if sc
      sc
    else
      @overwrite_system_config = Fluent::SystemConfig.new
      @overwrite_system_config
    end
  end
end
