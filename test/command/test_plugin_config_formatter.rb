require_relative '../helper'

require 'pathname'
require 'fluent/command/plugin_config_formatter'
require 'fluent/plugin/input'
require 'fluent/plugin/output'
require 'fluent/plugin/filter'
require 'fluent/plugin/parser'
require 'fluent/plugin/formatter'

class TestFluentPluginConfigFormatter < Test::Unit::TestCase
  class FakeInput < ::Fluent::Plugin::Input
    ::Fluent::Plugin.register_input("fake", self)
  end

  class FakeOutput < ::Fluent::Plugin::Output
    ::Fluent::Plugin.register_output("fake", self)

    def process(tag, es)
    end
  end

  class FakeFilter < ::Fluent::Plugin::Filter
    ::Fluent::Plugin.register_filter("fake", self)

    def filter(tag, time, record)
    end
  end

  class FakeParser < ::Fluent::Plugin::Parser
    ::Fluent::Plugin.register_parser("fake", self)
    def parse(text)
    end
  end

  class FakeFormatter < ::Fluent::Plugin::Formatter
    ::Fluent::Plugin.register_formatter("fake", self)
    def format(tag, time, record)
    end
  end

  sub_test_case "json" do
    data(input: [FakeInput, "input"],
         output: [FakeOutput, "output"],
         filter: [FakeFilter, "filter"],
         parser: [FakeParser, "parser"],
         formatter: [FakeFormatter, "formatter"])
    test "dumped config should be valid JSON" do |(klass, type)|
      dumped_config = capture_stdout do
        FluentPluginConfigFormatter.new(["--format=json", type, "fake"]).call
      end
      assert_equal(klass.dump_config_definition, JSON.parse(dumped_config)[klass.name])
    end
  end

  sub_test_case "arguments" do
    data do
      hash = {}
      ["input", "output", "filter", "parser", "formatter"].each do |type|
        ["txt", "json", "markdown"].each do |format|
          argv = ["--format=#{format}"]
          [
            ["--verbose", "--compact"],
            ["--verbose"],
            ["--compact"]
          ].each do |options|
            hash[(argv + options).join(" ")] = argv + options + [type, "fake"]
          end
        end
      end
      hash
    end
    test "dump txt" do |argv|
      capture_stdout do
        assert_nothing_raised do
          FluentPluginConfigFormatter.new(argv).call
        end
      end
    end
  end
end
