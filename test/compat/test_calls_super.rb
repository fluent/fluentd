require_relative '../helper'

# these are Fluent::Compat::* in fact
require 'fluent/input'
require 'fluent/output'
require 'fluent/filter'

class CompatCallsSuperTest < Test::Unit::TestCase
  class DummyGoodInput < Fluent::Input
    def configure(conf); super; end
    def start; super; end
    def before_shutdown; super; end
    def shutdown; super; end
  end
  class DummyBadInput < Fluent::Input
    def configure(conf); super; end
    def start; end
    def before_shutdown; end
    def shutdown; end
  end
  class DummyGoodOutput < Fluent::Output
    def configure(conf); super; end
    def start; super; end
    def before_shutdown; super; end
    def shutdown; super; end
  end
  class DummyBadOutput < Fluent::Output
    def configure(conf); super; end
    def start; end
    def before_shutdown; end
    def shutdown; end
  end
  class DummyGoodFilter < Fluent::Filter
    def configure(conf); super; end
    def filter(tag, time, record); end
    def start; super; end
    def before_shutdown; super; end
    def shutdown; super; end
  end
  class DummyBadFilter < Fluent::Filter
    def configure(conf); super; end
    def filter(tag, time, record); end
    def start; end
    def before_shutdown; end
    def shutdown; end
  end

  setup do
    Fluent::Test.setup
  end

  sub_test_case 'old API plugin which calls super properly' do
    test 'Input#start, #before_shutdown and #shutdown calls all superclass methods properly' do
      i = DummyGoodInput.new
      i.configure(config_element())
      assert i.configured?

      i.start
      assert i.started?

      i.before_shutdown
      assert i.before_shutdown?

      i.shutdown
      assert i.shutdown?

      assert i.log.out.logs.empty?
    end

    test 'Output#start, #before_shutdown and #shutdown calls all superclass methods properly' do
      i = DummyGoodOutput.new
      i.configure(config_element())
      assert i.configured?

      i.start
      assert i.started?

      i.before_shutdown
      assert i.before_shutdown?

      i.shutdown
      assert i.shutdown?

      assert i.log.out.logs.empty?
    end

    test 'Filter#start, #before_shutdown and #shutdown calls all superclass methods properly' do
      i = DummyGoodFilter.new
      i.configure(config_element())
      assert i.configured?

      i.start
      assert i.started?

      i.before_shutdown
      assert i.before_shutdown?

      i.shutdown
      assert i.shutdown?

      assert i.log.out.logs.empty?
    end
  end

  sub_test_case 'old API plugin which does not call super' do
    test 'Input#start, #before_shutdown and #shutdown calls superclass methods forcedly with logs' do
      i = DummyBadInput.new
      i.configure(config_element())
      assert i.configured?

      i.start
      assert i.started?

      i.before_shutdown
      assert i.before_shutdown?

      i.shutdown
      assert i.shutdown?

      logs = i.log.out.logs
      assert{ logs.any?{|l| l.include?("[warn]: super was not called in #start: called it forcedly plugin=CompatCallsSuperTest::DummyBadInput") } }
      assert{ logs.any?{|l| l.include?("[warn]: super was not called in #before_shutdown: calling it forcedly plugin=CompatCallsSuperTest::DummyBadInput") } }
      assert{ logs.any?{|l| l.include?("[warn]: super was not called in #shutdown: calling it forcedly plugin=CompatCallsSuperTest::DummyBadInput") } }
    end

    test 'Output#start, #before_shutdown and #shutdown calls superclass methods forcedly with logs' do
      i = DummyBadOutput.new
      i.configure(config_element())
      assert i.configured?

      i.start
      assert i.started?

      i.before_shutdown
      assert i.before_shutdown?

      i.shutdown
      assert i.shutdown?

      logs = i.log.out.logs
      assert{ logs.any?{|l| l.include?("[warn]: super was not called in #start: called it forcedly plugin=CompatCallsSuperTest::DummyBadOutput") } }
      assert{ logs.any?{|l| l.include?("[warn]: super was not called in #before_shutdown: calling it forcedly plugin=CompatCallsSuperTest::DummyBadOutput") } }
      assert{ logs.any?{|l| l.include?("[warn]: super was not called in #shutdown: calling it forcedly plugin=CompatCallsSuperTest::DummyBadOutput") } }
    end

    test 'Filter#start, #before_shutdown and #shutdown calls superclass methods forcedly with logs' do
      i = DummyBadFilter.new
      i.configure(config_element())
      assert i.configured?

      i.start
      assert i.started?

      i.before_shutdown
      assert i.before_shutdown?

      i.shutdown
      assert i.shutdown?

      logs = i.log.out.logs
      assert{ logs.any?{|l| l.include?("[warn]: super was not called in #start: called it forcedly plugin=CompatCallsSuperTest::DummyBadFilter") } }
      assert{ logs.any?{|l| l.include?("[warn]: super was not called in #before_shutdown: calling it forcedly plugin=CompatCallsSuperTest::DummyBadFilter") } }
      assert{ logs.any?{|l| l.include?("[warn]: super was not called in #shutdown: calling it forcedly plugin=CompatCallsSuperTest::DummyBadFilter") } }
    end
  end
end
