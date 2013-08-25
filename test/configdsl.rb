require 'fluent/config_dsl'

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

  def test_parse
    root = Fluent::Config::DSL::DSLParser.parse(TEST_DSL_CONFIG1)

    assert_equal 0, root.keys.size
    assert_equal 2, root.elements.size

    e0 = root.elements[0]
    assert_equal 'source', e0.name
    assert_equal '', e0.arg
    assert_equal 'forward', e0['type']
    assert_equal '24224', e0['port']

    e1 = root.elements[1]
    assert_equal 'match', e1.name
    assert_equal 'test.**', e1.arg
    assert_equal 'forward', e1['type']
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
end
