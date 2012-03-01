require 'helper'
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
    end

    module Checker
      extend self
      def format_check(tag, time, record); end
    end

    @@num = 0
    def create_register_output_name
      "mixin_text_#{@@num+=1}"
    end

    def format_check(hash, tagname = 'test')
      mock(Checker).format_check(tagname, @time.to_i, hash)
    end

    def create_driver(include_klass, conf = '')
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

      Fluent::Test::BufferedOutputTestDriver.new(klass) {
      }.configure("tyep #{register_output_name}" + conf)
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
  end

  class HandleTagMixinTest < Test::Unit::TestCase
    include Utils

    def test_add_tag_prefix
      format_check({
        'a' => 1
      }, 'tag_prefix.test')

      d = create_driver(Fluent::HandleTagNameMixin, %[
        add_tag_prefix tag_prefix.
        include_tag_key true
      ])

      d.emit({'a' => 1})
      d.run
    end

    def test_add_tag_suffix
      format_check({
        'a' => 1
      }, 'test.test_suffix')

      d = create_driver(Fluent::HandleTagNameMixin, %[
        add_tag_suffix .test_suffix
        include_tag_key true
      ])

      d.emit({'a' => 1})
      d.run
    end

    def test_remove_tag_prefix
      format_check({
        'a' => 1
      }, 'st')

      d = create_driver(Fluent::HandleTagNameMixin, %[
        remove_tag_prefix te
        include_tag_key true
      ])

      d.emit({'a' => 1})
      d.run
    end

    def test_remove_tag_suffix
      format_check({
        'a' => 1
      }, 'te')

      d = create_driver(Fluent::HandleTagNameMixin, %[
        remove_tag_suffix st
        include_tag_key true
      ])

      d.emit({'a' => 1})
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
