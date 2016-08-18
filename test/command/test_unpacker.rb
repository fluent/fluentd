require_relative '../helper'
require 'fluent/command/unpacker'
require 'flexmock/test_unit'

class TestFluentUnpacker < ::Test::Unit::TestCase
  def supress_stdout
    out = StringIO.new
    $stdout = out
    yield
  ensure
    $stdout = STDOUT
  end

  module ::Command
    class Dummy < Base
      def call; end
    end
  end

  sub_test_case 'call' do
    data(
      empty: [],
      invalid: %w(invalid packed.log),
    )
    test 'should fail when invalid command' do |argv|
      fu = FluentUnpacker.new(argv)

      assert_raise(SystemExit) do
        supress_stdout { fu.call }
      end
    end

    data(
      cat: %w(cat packed.log),
      head: %w(head packed.log),
      formats: %w(formats packed.log)
    )
    test 'should success when valid command' do |argv|
      fu = FluentUnpacker.new(argv)

      flexstub(::Command) do |command|
        command.should_receive(:const_get).once.and_return(::Command::Dummy)
        assert_nothing_raised do
          fu.call
        end
      end
    end
  end
end
