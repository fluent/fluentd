require_relative '../helper'
require 'fluent/plugin/base'
require 'fluent/plugin/input'
require 'fluent/plugin/owned_by_mixin'

module OwnedByMixinTestEnv
  class DummyParent < Fluent::Plugin::Input
    Fluent::Plugin.register_input('dummy_parent', self)
  end
  class DummyChild < Fluent::Plugin::Base
    include Fluent::Plugin::OwnedByMixin
    Fluent::Plugin.register_parser('dummy_child', self)
  end
end

class OwnedByMixinTest < Test::Unit::TestCase
  sub_test_case 'Owned plugins' do
    setup do
      Fluent::Test.setup
    end

    test 'inherits plugin id and logger from parent' do
      parent = Fluent::Plugin.new_input('dummy_parent')
      parent.configure(config_element('ROOT', '', {'@id' => 'my_parent_id', '@log_level' => 'trace'}))
      child = Fluent::Plugin.new_parser('dummy_child', parent: parent)

      assert_equal parent.object_id, child.owner.object_id

      assert child.instance_eval{ @_plugin_id_configured }
      assert_equal 'my_parent_id', child.instance_eval{ @_plugin_id }

      assert_equal Fluent::Log::LEVEL_TRACE, child.log.level
    end
  end
end
