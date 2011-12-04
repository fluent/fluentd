require 'helper'
require 'fluent/mixin'

#class MixinTest < Test::Unit::TestCase
#  class MixinOutputTester < Fluent::BufferedOutput
#    Fluent::Plugin.register_output('mixintest', self)
#    include Fluent::PlainTextFormatterMixin
#    def configure(conf)
#      super
#    end
#    def write(chunk)
#      chunk.read
#    end
#  end
#
#  def create_driver(conf='')
#    Fluent::Test::BufferedOutputTestDriver.new(MixinOutputTester).configure(conf)
#  end
#
#  def test_default_config
#    d = create_driver
#    assert_equal true, d.instance.output_include_time
#    assert_equal true, d.instance.output_include_tag
#    assert_equal 'json', d.instance.output_data_type
#    assert_equal "\t", d.instance.output_field_separator
#    assert_equal true, d.instance.output_add_newline
#    assert_equal nil, d.instance.instance_eval{@localtime}
#
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#    d.emit({"foo"=>1,"bar"=>501}, time)
#    d.emit({"foo"=>2,"bar"=>502}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z\ttest\t{"foo":1,"bar":501}\n2011-11-29T12:02:50Z\ttest\t{"foo":2,"bar":502}\n], text
#  end
#
#  def test_timezone
#    d = create_driver %[
#utc
#]
#    assert_equal false, d.instance.instance_eval{@localtime}
#
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#    d.emit({"foo"=>1,"bar"=>501}, time)
#    d.emit({"foo"=>2,"bar"=>502}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z\ttest\t{"foo":1,"bar":501}\n2011-11-29T12:02:50Z\ttest\t{"foo":2,"bar":502}\n], text
#
#    d = create_driver %[
#localtime
#]
#    assert_equal true, d.instance.instance_eval{@localtime}
#
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#    time_s = Time.parse("2011-11-29 12:02:50 UTC").getlocal.iso8601
#    d.emit({"foo"=>1,"bar"=>501}, time)
#    d.emit({"foo"=>2,"bar"=>502}, time)
#    text = d.run
#    assert_equal time_s + %[\ttest\t{"foo":1,"bar":501}\n] + time_s + %[\ttest\t{"foo":2,"bar":502}\n], text
#  end
#
#  def test_time_tag_onoff
#    d = create_driver %[
#output_include_time true
#output_include_tag false
#]
#    assert_equal true, d.instance.output_include_time
#    assert_equal false, d.instance.output_include_tag
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#    d.emit({"foo"=>1,"bar"=>501}, time)
#    d.emit({"foo"=>2,"bar"=>502}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z\t{"foo":1,"bar":501}\n2011-11-29T12:02:50Z\t{"foo":2,"bar":502}\n], text
#
#    d = create_driver %[
#output_include_time false
#output_include_tag true
#]
#    assert_equal false, d.instance.output_include_time
#    assert_equal true, d.instance.output_include_tag
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#    d.emit({"foo"=>1,"bar"=>501}, time)
#    d.emit({"foo"=>2,"bar"=>502}, time)
#    text = d.run
#    assert_equal %[test\t{"foo":1,"bar":501}\ntest\t{"foo":2,"bar":502}\n], text
#    
#    d = create_driver %[
#output_include_time false
#output_include_tag false
#]
#    assert_equal false, d.instance.output_include_time
#    assert_equal false, d.instance.output_include_tag
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#    d.emit({"foo"=>1,"bar"=>501}, time)
#    d.emit({"foo"=>2,"bar"=>502}, time)
#    text = d.run
#    assert_equal %[{"foo":1,"bar":501}\n{"foo":2,"bar":502}\n], text
#  end
#
#  def test_data_type
#    d = create_driver %[
#output_data_type json
#]
#    assert_equal 'json', d.instance.output_data_type
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#    d.emit({"foo"=>1,"bar"=>"This is what you want"}, time)
#    d.emit({"foo"=>2,"bar"=>"Is this what you want or not"}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z\ttest\t{"foo":1,"bar":"This is what you want"}\n2011-11-29T12:02:50Z\ttest\t{"foo":2,"bar":"Is this what you want or not"}\n], text
#
#    d = create_driver %[
#output_data_type attr:foo
#]
#    assert_equal 'attr:foo', d.instance.output_data_type
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#    d.emit({"foo"=>1,"bar"=>"This is what you want"}, time)
#    d.emit({"foo"=>2,"bar"=>"Is this what you want or not"}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z\ttest\t1\n2011-11-29T12:02:50Z\ttest\t2\n], text
#
#    d = create_driver %[
#output_data_type attr:bar
#]
#    assert_equal 'attr:bar', d.instance.output_data_type
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#    d.emit({"foo"=>1,"bar"=>"This is what you want"}, time)
#    d.emit({"foo"=>2,"bar"=>"Is this what you want or not"}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z\ttest\tThis is what you want\n2011-11-29T12:02:50Z\ttest\tIs this what you want or not\n], text
#
#    d = create_driver %[
#output_data_type attr:foo,bar
#]
#    assert_equal 'attr:foo,bar', d.instance.output_data_type
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#    d.emit({"foo"=>1,"bar"=>"This is what you want"}, time)
#    d.emit({"foo"=>2,"bar"=>"Is this what you want or not"}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z\ttest\t1\tThis is what you want\n2011-11-29T12:02:50Z\ttest\t2\tIs this what you want or not\n], text
#  end
#
#  def test_add_newline
#    d = create_driver %[
#output_add_newline false
#]
#    assert_equal false, d.instance.output_add_newline
#
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#    d.emit({"foo"=>1,"bar"=>"This is what you want"}, time)
#    d.emit({"foo"=>2,"bar"=>"Is this what you want or not"}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z\ttest\t{"foo":1,"bar":"This is what you want"}2011-11-29T12:02:50Z\ttest\t{"foo":2,"bar":"Is this what you want or not"}], text
#    
#    d = create_driver %[
#output_add_newline false
#]
#    assert_equal false, d.instance.output_add_newline
#
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#    d.emit({"foo"=>1,"bar"=>"This is what you want\n"}, time)
#    d.emit({"foo"=>2,"bar"=>"Is this what you want or not\n"}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z\ttest\t{"foo":1,"bar":"This is what you want\\n"}2011-11-29T12:02:50Z\ttest\t{"foo":2,"bar":"Is this what you want or not\\n"}], text
#
#    d = create_driver %[
#output_data_type attr:bar
#output_add_newline false
#]
#    assert_equal false, d.instance.output_add_newline
#
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#    d.emit({"foo"=>1,"bar"=>"This is what you want\n"}, time)
#    d.emit({"foo"=>2,"bar"=>"Is this what you want or not\n"}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z\ttest\tThis is what you want\n2011-11-29T12:02:50Z\ttest\tIs this what you want or not\n], text
#
#    d = create_driver %[
#output_data_type attr:bar
#output_add_newline true
#]
#    assert_equal true, d.instance.output_add_newline
#
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#    d.emit({"foo"=>1,"bar"=>"This is what you want\n"}, time)
#    d.emit({"foo"=>2,"bar"=>"Is this what you want or not\n"}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z\ttest\tThis is what you want\n\n2011-11-29T12:02:50Z\ttest\tIs this what you want or not\n\n], text
#  end
#
#  def test_field_separator
#    time = Time.parse("2011-11-29 12:02:50 UTC").to_i
#
#    d = create_driver %[
#output_field_separator SPACE
#]
#    assert_equal " ", d.instance.output_field_separator
#    d.emit({"foo"=>1,"bar"=>501}, time)
#    d.emit({"foo"=>2,"bar"=>502}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z test {"foo":1,"bar":501}\n2011-11-29T12:02:50Z test {"foo":2,"bar":502}\n], text
#
#    d = create_driver %[
#output_field_separator SPACE
#output_data_type attr:bar,foo
#]
#    assert_equal " ", d.instance.output_field_separator
#    d.emit({"foo"=>1,"bar"=>501}, time)
#    d.emit({"foo"=>2,"bar"=>502}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z test 501 1\n2011-11-29T12:02:50Z test 502 2\n], text
#
#    d = create_driver %[
#output_field_separator COMMA
#]
#    assert_equal ",", d.instance.output_field_separator
#    d.emit({"foo"=>1,"bar"=>501}, time)
#    d.emit({"foo"=>2,"bar"=>502}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z,test,{"foo":1,"bar":501}\n2011-11-29T12:02:50Z,test,{"foo":2,"bar":502}\n], text
#
#    d = create_driver %[
#output_field_separator COMMA
#output_data_type attr:foo,bar
#]
#    assert_equal ",", d.instance.output_field_separator
#    d.emit({"foo"=>1,"bar"=>501}, time)
#    d.emit({"foo"=>2,"bar"=>502}, time)
#    text = d.run
#    assert_equal %[2011-11-29T12:02:50Z,test,1,501\n2011-11-29T12:02:50Z,test,2,502\n], text
#  end
#
#end

