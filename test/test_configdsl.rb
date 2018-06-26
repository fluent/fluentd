require_relative 'helper'
require 'fluent/config/dsl'
require 'fluent/test'

class ConfigDSLTest < Test::Unit::TestCase
  # TEST_CONFIG1 = %[
  #   <source>
  #     type forward
  #     port 24224
  #   </source>
  #   <match test.**>
  #     type forward
  #     flush_interval 1s
  #     <server>
  #       host host0.example.com
  #       port 24224
  #     </server>
  #     <server>
  #       host host1.example.com
  #       port 24224
  #     </server>
  #   </match>
  # ]
  TEST_DSL_CONFIG1 = %q[
source {
  type "forward"
  port 24224
}
match('test.**') {
  type "forward"
  flush_interval "1s"
  (0..1).each do |i|
    server {
      host "host#{i}.example.com"
      port 24224
    }
  end
}
]

  TEST_DSL_CONFIG2 = %q[
v = [0, 1, 2]
]

  TEST_DSL_CONFIG3 = %q[
match
]

  TEST_DSL_CONFIG4 = %q[
match('aa', 'bb'){
  type :null
}
]

  TEST_DSL_CONFIG5 = %q[
match('aa')
]

  def test_parse
    root = Fluent::Config::DSL::Parser.parse(TEST_DSL_CONFIG1)

    assert_equal 0, root.keys.size
    assert_equal 2, root.elements.size

    e0 = root.elements[0]
    assert_equal 'source', e0.name
    assert_equal '', e0.arg
    assert_equal 'forward', e0['@type']
    assert_equal '24224', e0['port']

    e1 = root.elements[1]
    assert_equal 'match', e1.name
    assert_equal 'test.**', e1.arg
    assert_equal 'forward', e1['@type']
    assert_equal '1s', e1['flush_interval']
    assert_equal 2, e1.elements.size
    e1s0 = e1.elements[0]
    assert_equal 'server', e1s0.name
    assert_equal 'host0.example.com', e1s0['host']
    assert_equal '24224', e1s0['port']
    e1s1 = e1.elements[1]
    assert_equal 'server', e1s1.name
    assert_equal 'host1.example.com', e1s1['host']
    assert_equal '24224', e1s1['port']
  end

  def test_parse2
    root = Fluent::Config::DSL::Parser.parse(TEST_DSL_CONFIG2)

    assert_equal 0, root.keys.size
    assert_equal 0, root.elements.size
  end

  def test_config_error
    assert_raise(ArgumentError) {
      Fluent::Config::DSL::Parser.parse(TEST_DSL_CONFIG3)
    }

    assert_raise(ArgumentError) {
      Fluent::Config::DSL::Parser.parse(TEST_DSL_CONFIG4)
    }

    assert_raise(ArgumentError) {
      Fluent::Config::DSL::Parser.parse(TEST_DSL_CONFIG5)
    }
  end

  def test_with_ruby_keyword
    uname_string = `uname -a`
    root1 = Fluent::Config::DSL::Parser.parse(<<DSL)
uname_str = ruby.open('|uname -a'){|out| out.read}
source {
  uname uname_str
}
DSL
    source1 = root1.elements.first
    assert_equal 'source', source1.name
    assert_equal 1, source1.keys.size
    assert_equal uname_string, source1['uname']

    root2 = Fluent::Config::DSL::Parser.parse(<<DSL)
ruby_version = ruby {
  require 'erb'
  ERB.new('<%= RUBY_VERSION %> from erb').result
}
source {
  version ruby_version
}
DSL
    source2 = root2.elements.first
    assert_equal 'source', source2.name
    assert_equal 1, source2.keys.size
    assert_equal "#{RUBY_VERSION} from erb", source2['version']

    # Parser#parse raises NoMethodError when configuration dsl elements are written in ruby block
    conf3 = <<DSL
ruby {
  source {
    type "tail"
  }
}
source {
  uname uname_str
}
DSL
    assert_raise (NoMethodError) { Fluent::Config::DSL::Parser.parse(conf3) }
  end
end
