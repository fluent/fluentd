require File.dirname(__FILE__) + '/helper'

require 'fluent/match'

class MatchTest < Test::Unit::TestCase
  include Fluent

  def test_simple
    assert_match('a', 'a')
    assert_match('a.b', 'a.b')
    assert_not_match('a', 'b')
    assert_not_match('a.b', 'aab')
  end

  def test_wildcard
    assert_match('a*', 'a')
    assert_match('a*', 'ab')
    assert_match('a*', 'abc')

    assert_match('*a', 'a')
    assert_match('*a', 'ba')
    assert_match('*a', 'cba')

    assert_match('*a*', 'a')
    assert_match('*a*', 'ba')
    assert_match('*a*', 'ac')
    assert_match('*a*', 'bac')

    assert_not_match('a*', 'a.b')
    assert_not_match('a*', 'ab.c')
    assert_not_match('a*', 'ba')
    assert_not_match('*a', 'ab')

    assert_match('a.*', 'a.b')
    assert_match('a.*', 'a.c')
    assert_not_match('a.*', 'ab')

    assert_match('a.*.c', 'a.b.c')
    assert_match('a.*.c', 'a.c.c')
    assert_not_match('a.*.c', 'a.c')
  end

  def test_recursive_wildcard
    assert_match('a.**', 'a')
    assert_not_match('a.**', 'ab')
    assert_not_match('a.**', 'abc')
    assert_match('a.**', 'a.b')
    assert_not_match('a.**', 'ab.c')
    assert_not_match('a.**', 'ab.d.e')

    assert_match('a**', 'a')
    assert_match('a**', 'ab')
    assert_match('a**', 'abc')
    assert_match('a**', 'a.b')
    assert_match('a**', 'ab.c')
    assert_match('a**', 'ab.d.e')

    assert_match('**.a', 'a')
    assert_not_match('**.a', 'ba')
    assert_not_match('**.a', 'c.ba')
    assert_match('**.a', 'b.a')
    assert_match('**.a', 'cb.a')
    assert_match('**.a', 'd.e.a')

    assert_match('**a', 'a')
    assert_match('**a', 'ba')
    assert_match('**a', 'c.ba')
    assert_match('**a', 'b.a')
    assert_match('**a', 'cb.a')
    assert_match('**a', 'd.e.a')
  end

  def test_or
    assert_match('a.{b,c}', 'a.b')
    assert_match('a.{b,c}', 'a.c')
    assert_not_match('a.{b,c}', 'a.d')

    assert_match('a.{b,c}.**', 'a.b')
    assert_match('a.{b,c}.**', 'a.c')
    assert_not_match('a.{b,c}.**', 'a.d')
    assert_not_match('a.{b,c}.**', 'a.cd')

    assert_match('a.{b.**,c}', 'a.b')
    assert_match('a.{b.**,c}', 'a.b.c')
    assert_match('a.{b.**,c}', 'a.c')
    assert_not_match('a.{b.**,c}', 'a.c.d')
  end

  #def test_character_class
  #  assert_match('[a]', 'a')
  #  assert_match('[ab]', 'a')
  #  assert_match('[ab]', 'b')
  #  assert_not_match('[ab]', 'c')
  #
  #  assert_match('[a-b]', 'a')
  #  assert_match('[a-b]', 'a')
  #  assert_match('[a-b]', 'b')
  #  assert_not_match('[a-b]', 'c')
  #
  #  assert_match('[a-b0-9]', 'a')
  #  assert_match('[a-b0-9]', '0')
  #  assert_not_match('[a-b0-9]', 'c')
  #end

  def assert_match(pat, str)
    m = GlobMatchPattern.new(pat)
    assert_equal true, m.match(str)
  end

  def assert_not_match(pat, str)
    m = GlobMatchPattern.new(pat)
    assert_equal false, m.match(str)
  end
end

