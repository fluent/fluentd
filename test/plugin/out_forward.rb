require 'fluent/test'

class ForwardOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    send_timeout 51
    password pass
    compression_level 9
    <server>
      name test
      host 127.0.0.1
      port 13999
    </server>
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::OutputTestDriver.new(Fluent::ForwardOutput) do
      def write(chunk)
        chunk.read
      end
    end.configure(conf)
  end

  def test_configure
    d = create_driver
    nodes = d.instance.nodes
    assert_equal 51, d.instance.send_timeout
    assert_equal "pass", d.instance.password
    assert_equal 9, d.instance.compression_level
    assert_equal 1, nodes.length
    node = nodes.first
    assert_equal "test", node.name
    assert_equal '127.0.0.1', node.host
    assert_equal 13999, node.port
  end
end
