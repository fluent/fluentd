require_relative 'helper'
require 'fluent/mixin'
require 'fluent/env'
require 'fluent/plugin'
require 'fluent/config'
require 'fluent/test'
require 'timecop'

module MixinTest
  module Utils
    def setup
      super
      Fluent::Test.setup
      @time = Time.utc(1,2,3,4,5,2010,nil,nil,nil,nil)
      Timecop.freeze(@time)
    end

    def teardown
      super
      Timecop.return
      GC.start
    end

    module Checker
      extend self
      def format_check(tag, time, record); end
    end

    @@num = 0

    def create_register_output_name
      @@num += 1
      "mixin_text_#{@@num}"
    end

    def format_check(hash, tagname = 'test')
      mock(Checker).format_check(tagname, @time.to_i, hash)
    end

    def create_driver(include_klass, conf = '', tag = "test", &block)
      register_output_name = create_register_output_name
      include_klasses = [include_klass].flatten

      klass = Class.new(Fluent::BufferedOutput) {
        include_klasses.each {|k| include k }

        Fluent::Plugin.register_output(register_output_name, self)
        def format(tag, time, record)
          Checker.format_check(tag, time, record)
          [tag, time, record].to_msgpack
        end

        def write(chunk); end
      }

      if block
        Utils.const_set("MixinTestClass#{@@num}", klass)
        klass.module_eval(&block)
      end

      Fluent::Test::BufferedOutputTestDriver.new(klass, tag) {
      }.configure("type #{register_output_name}" + conf)
    end
  end

  class SetTagKeyMixinText < Test::Unit::TestCase
    include Utils

    def test_tag_key_default
      format_check({
        'a' => 1
      })

      d = create_driver(Fluent::SetTagKeyMixin, %[
      ])
      d.emit({'a' => 1})
      d.run
    end

    def test_include_tag_key_true
      format_check({
        'tag' => 'test',
        'a' => 1
      })

      d = create_driver(Fluent::SetTagKeyMixin, %[
      include_tag_key true
      ])

      d.emit({'a' => 1})
      d.run
    end

    def test_include_tag_key_false
      format_check({
        'a' => 1
      })

      d = create_driver(Fluent::SetTagKeyMixin, %[
      include_tag_key false
      ])
      d.emit({'a' => 1})
      d.run
    end

    def test_tag_key_set
      format_check({
        'tag_key_changed' => 'test',
        'a' => 1
      })

      d = create_driver(Fluent::SetTagKeyMixin, %[
      include_tag_key true
      tag_key tag_key_changed
      ])

      d.emit({'a' => 1})
      d.run
    end

    sub_test_case "mixin" do
      data(
        'true' => true,
        'false' => false)
      test 'include_tag_key' do |param|
        d = create_driver(Fluent::SetTagKeyMixin) {
          config_set_default :include_tag_key, param
        }

        assert_equal(param, d.instance.include_tag_key)
      end
    end
  end

  class SetTimeKeyMixinText < Test::Unit::TestCase
    include Utils

    def test_time_key_default
      format_check({
        'a' => 1
      })

      d = create_driver(Fluent::SetTimeKeyMixin, %[
      ])

      d.emit({'a' => 1})
      d.run
    end

    def test_include_time_key_true
      format_check({
        'time' => "2010-05-04T03:02:01Z",
        'a' => 1
      })

      d = create_driver(Fluent::SetTimeKeyMixin, %[
      include_time_key true
      ])

      d.emit({'a' => 1})
      d.run
    end

    def test_time_format
      format_check({
        'time' => "20100504",
        'a' => 1
      })

      d = create_driver(Fluent::SetTimeKeyMixin, %[
      include_time_key true
      time_format %Y%m%d
      ])

      d.emit({'a' => 1})
      d.run
    end

    def test_timezone_1
      format_check({
        'time' => "2010-05-03T17:02:01-10:00",
        'a' => 1
      })

      d = create_driver(Fluent::SetTimeKeyMixin, %[
      include_time_key true
      timezone Pacific/Honolulu
      ])

      d.emit({'a' => 1})
      d.run
    end

    def test_timezone_2
      format_check({
        'time' => "2010-05-04T08:32:01+05:30",
        'a' => 1
      })

      d = create_driver(Fluent::SetTimeKeyMixin, %[
      include_time_key true
      timezone +05:30
      ])

      d.emit({'a' => 1})
      d.run
    end

    def test_timezone_invalid
      assert_raise(Fluent::ConfigError) do
        create_driver(Fluent::SetTimeKeyMixin, %[
        include_time_key true
        timezone Invalid/Invalid
        ])
      end
    end

    sub_test_case "mixin" do
      data(
        'true' => true,
        'false' => false)
      test 'include_time_key' do |param|
        d = create_driver(Fluent::SetTimeKeyMixin) {
          config_set_default :include_time_key, param
        }

        assert_equal(param, d.instance.include_time_key)
      end
    end
  end

  class HandleTagMixinTest < Test::Unit::TestCase
    include Utils

    def test_add_tag_prefix
      format_check({
        'a' => 1
      }, 'tag_prefix.test')
      format_check({
        'a' => 2
      }, 'tag_prefix.test')

      d = create_driver(Fluent::HandleTagNameMixin, %[
        add_tag_prefix tag_prefix.
        include_tag_key true
      ])

      d.emit({'a' => 1})
      d.emit({'a' => 2})
      d.run
    end

    def test_add_tag_suffix
      format_check({
        'a' => 1
      }, 'test.test_suffix')
      format_check({
        'a' => 2
      }, 'test.test_suffix')

      d = create_driver(Fluent::HandleTagNameMixin, %[
        add_tag_suffix .test_suffix
        include_tag_key true
      ])

      d.emit({'a' => 1})
      d.emit({'a' => 2})
      d.run
    end

    def test_remove_tag_prefix
      format_check({
        'a' => 1
      }, 'test')
      format_check({
        'a' => 2
      }, 'test')

      d = create_driver(Fluent::HandleTagNameMixin, %[
        remove_tag_prefix te
        include_tag_key true
      ], "tetest")

      d.emit({'a' => 1})
      d.emit({'a' => 2})
      d.run
    end

    def test_remove_tag_suffix
      format_check({
        'a' => 1
      }, 'test')
      format_check({
        'a' => 2
      }, 'test')

      d = create_driver(Fluent::HandleTagNameMixin, %[
        remove_tag_suffix st
        include_tag_key true
      ], "testst")

      d.emit({'a' => 1})
      d.emit({'a' => 2})
      d.run
    end

    def test_mix_tag_handle
      format_check({
        'a' => 1
      }, 'prefix.t')

      d = create_driver(Fluent::HandleTagNameMixin, %[
        remove_tag_prefix tes
        add_tag_prefix prefix.
      ])

      d.emit({'a' => 1})
      d.run
    end

    def test_with_set_tag_key_mixin
      format_check({
        'tag' => 'tag_prefix.test',
        'a' => 1
      }, 'tag_prefix.test')

      d = create_driver([Fluent::SetTagKeyMixin, Fluent::HandleTagNameMixin], %[
        add_tag_prefix tag_prefix.
        include_tag_key true
      ])

      d.emit({'a' => 1})
      d.run
    end

    def test_with_set_tag_key_mixin_include_order_reverse
      format_check({
        'tag' => 'tag_prefix.test',
        'a' => 1
      }, 'tag_prefix.test')

      d = create_driver([Fluent::HandleTagNameMixin, Fluent::SetTagKeyMixin], %[
        add_tag_prefix tag_prefix.
        include_tag_key true
      ])

      d.emit({'a' => 1})
      d.run
    end
  end
end
