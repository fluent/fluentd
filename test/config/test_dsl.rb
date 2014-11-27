require_relative '../helper'
require 'fluent/config/element'
require "fluent/config/dsl"

DSL_CONFIG_EXAMPLE = %q[
worker {
  hostname = "myhostname"

  (0..9).each { |i|
    source {
      type :tail
      path "/var/log/httpd/access.part#{i}.log"

      filter ('bar.**') {
        type :hoge
        val1 "moge"
        val2 ["foo", "bar", "baz"]
        val3 10
        id :hoge

        subsection {
          foo "bar"
        }
        subsection {
          foo "baz"
        }
      }

      filter ('foo.**') {
        type "pass"
      }

      match ('{foo,bar}.**') {
        type "file"
        path "/var/log/httpd/access.#{hostname}.#{i}.log"
      }
    }
  }
}
]

DSL_CONFIG_EXAMPLE_WITHOUT_WORKER = %q[
hostname = "myhostname"

source {
  type :tail
  path "/var/log/httpd/access.part.log"

  element {
    name "foo"
  }

  match ('{foo,bar}.**') {
    type "file"
    path "/var/log/httpd/access.full.log"
  }
}
]

DSL_CONFIG_RETURNS_NON_ELEMENT = %q[
worker {
}
[]
]

DSL_CONFIG_WRONG_SYNTAX1 = %q[
match
]
DSL_CONFIG_WRONG_SYNTAX2 = %q[
match('aa','bb'){
  type :null
}
]
DSL_CONFIG_WRONG_SYNTAX3 = %q[
match('aa','bb')
]

module Fluent::Config
  class TestDSLParser < ::Test::Unit::TestCase
    sub_test_case 'with worker tag on top level' do
      def setup
        @root = Fluent::Config::DSL::Parser.parse(DSL_CONFIG_EXAMPLE, 'dsl_config.rb')
      end

      sub_test_case '.parse' do
        test 'makes root element' do
          assert_equal('ROOT', @root.name)
          assert_predicate(@root.arg, :empty?)
          assert_equal(0, @root.keys.size)
        end

        test 'makes worker element for worker tag' do
          assert_equal(1, @root.elements.size)

          worker = @root.elements.first
          assert_equal('worker', worker.name)
          assert_predicate(worker.arg, :empty?)
          assert_equal(0, worker.keys.size)
          assert_equal(10, worker.elements.size)
        end

        test 'makes subsections for blocks, with variable substitution' do
          ele4 = @root.elements.first.elements[4]

          assert_equal('source', ele4.name)
          assert_predicate(ele4.arg, :empty?)
          assert_equal(2, ele4.keys.size)
          assert_equal('tail', ele4['type'])
          assert_equal("/var/log/httpd/access.part4.log", ele4['path'])
        end

        test 'makes user-defined sections with blocks' do
          filter0 = @root.elements.first.elements[4].elements.first

          assert_equal('filter', filter0.name)
          assert_equal('bar.**', filter0.arg)
          assert_equal('hoge', filter0['type'])
          assert_equal('moge', filter0['val1'])
          assert_equal(JSON.dump(['foo', 'bar', 'baz']), filter0['val2'])
          assert_equal('10', filter0['val3'])
          assert_equal('hoge', filter0['id'])

          assert_equal(2, filter0.elements.size)
          assert_equal('subsection', filter0.elements[0].name)
          assert_equal('bar', filter0.elements[0]['foo'])
          assert_equal('subsection', filter0.elements[1].name)
          assert_equal('baz', filter0.elements[1]['foo'])
        end

        test 'makes values with user-assigned variable substitutions' do
          match0 = @root.elements.first.elements[4].elements.last

          assert_equal('match', match0.name)
          assert_equal('{foo,bar}.**', match0.arg)
          assert_equal('file', match0['type'])
          assert_equal('/var/log/httpd/access.myhostname.4.log', match0['path'])
        end
      end
    end

    sub_test_case 'without worker tag on top level' do
      def setup
        @root = Fluent::Config::DSL::Parser.parse(DSL_CONFIG_EXAMPLE_WITHOUT_WORKER, 'dsl_config_without_worker.rb')
      end

      sub_test_case '.parse' do
        test 'makes root element' do
          assert_equal('ROOT', @root.name)
          assert_predicate(@root.arg, :empty?)
          assert_equal(0, @root.keys.size)
        end

        test 'does not make worker element implicitly because DSL configuration does not support v10 compat mode' do
          assert_equal(1, @root.elements.size)
          assert_equal('source', @root.elements.first.name)
          refute(@root.elements.find { |e| e.name == 'worker' })
        end
      end
    end

    sub_test_case 'with configuration that returns non element on top' do
      sub_test_case '.parse' do
        test 'does not crash' do
          root = Fluent::Config::DSL::Parser.parse(DSL_CONFIG_RETURNS_NON_ELEMENT, 'dsl_config_returns_non_element.rb')
        end
      end
    end

    sub_test_case 'with configuration with wrong arguments for specific elements' do
      sub_test_case '.parse' do
        test 'raises ArgumentError correctly' do
          assert_raise(ArgumentError) { Fluent::Config::DSL::Parser.parse(DSL_CONFIG_WRONG_SYNTAX1, 'dsl_config_wrong_syntax1') }
          assert_raise(ArgumentError) { Fluent::Config::DSL::Parser.parse(DSL_CONFIG_WRONG_SYNTAX2, 'dsl_config_wrong_syntax1') }
          assert_raise(ArgumentError) { Fluent::Config::DSL::Parser.parse(DSL_CONFIG_WRONG_SYNTAX3, 'dsl_config_wrong_syntax1') }
        end
      end
    end

    sub_test_case 'with ruby keyword, that provides ruby Kernel module features' do
      sub_test_case '.parse' do
        test 'can get result of Kernel.open() by ruby.open()' do
          uname_string = `uname -a`
          root = Fluent::Config::DSL::Parser.parse(<<DSL)
worker {
  uname_str = ruby.open('|uname -a'){|out| out.read}
  source {
    uname uname_str
  }
}
DSL
          worker = root.elements.first
          assert_equal('worker', worker.name)
          source = worker.elements.first
          assert_equal('source', source.name)
          assert_equal(1, source.keys.size)
          assert_equal(uname_string, source['uname'])
        end

        test 'accepts ruby keyword with block, which allow to use methods included from ::Kernel' do
          root = Fluent::Config::DSL::Parser.parse(<<DSL)
worker {
  ruby_version = ruby {
    require 'erb'
    ERB.new('<%= RUBY_VERSION %> from erb').result
  }
  source {
    version ruby_version
  }
}
DSL
          worker = root.elements.first
          assert_equal('worker', worker.name)
          source = worker.elements.first
          assert_equal('source', source.name)
          assert_equal(1, source.keys.size)
          assert_equal("#{RUBY_VERSION} from erb", source['version'])
        end

        test 'raises NoMethodError when configuration DSL elements are written in ruby block' do
          conf = <<DSL
worker {
  ruby {
    source {
      type "tail"
    }
  }
  source {
    uname uname_str
  }
}
DSL
          assert_raise(NoMethodError) { Fluent::Config::DSL::Parser.parse(conf) }
        end
      end
    end
  end
end
