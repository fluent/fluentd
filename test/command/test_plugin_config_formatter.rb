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

    desc "path to something"
    config_param :path, :string
  end

  class FakeOutput < ::Fluent::Plugin::Output
    ::Fluent::Plugin.register_output("fake", self)

    desc "path to something"
    config_param :path, :string

    def process(tag, es)
    end
  end

  class FakeFilter < ::Fluent::Plugin::Filter
    ::Fluent::Plugin.register_filter("fake", self)

    desc "path to something"
    config_param :path, :string

    def filter(tag, time, record)
    end
  end

  class FakeParser < ::Fluent::Plugin::Parser
    ::Fluent::Plugin.register_parser("fake", self)

    desc "path to something"
    config_param :path, :string

    def parse(text)
    end
  end

  class FakeFormatter < ::Fluent::Plugin::Formatter
    ::Fluent::Plugin.register_formatter("fake", self)

    desc "path to something"
    config_param :path, :string

    def format(tag, time, record)
    end
  end

  class SimpleInput < ::Fluent::Plugin::Input
    ::Fluent::Plugin.register_input("simple", self)
    helpers :inject, :compat_parameters

    desc "path to something"
    config_param :path, :string
  end

  class ComplexOutput < ::Fluent::Plugin::Output
    ::Fluent::Plugin.register_output("complex", self)
    helpers :inject, :compat_parameters

    config_section :authentication, required: true, multi: false do
      desc "username"
      config_param :username, :string
      desc "password"
      config_param :passowrd, :string, secret: true
    end

    config_section :parent do
      config_section :child do
        desc "names"
        config_param :names, :array
        desc "difficulty"
        config_param :difficulty, :enum, list: [:easy, :normal, :hard], default: :normal
      end
    end

    def process(tag, es)
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
      expected = {
        path: {
          desc: "path to something",
          type: "string",
          required: true
        }
      }
      assert_equal(expected, JSON.parse(dumped_config, symbolize_names: true)[klass.name.to_sym])
    end
  end

  sub_test_case "text" do
    test "input simple" do
      dumped_config = capture_stdout do
        FluentPluginConfigFormatter.new(["--format=txt", "input", "simple"]).call
      end
      expected = <<TEXT
helpers: inject,compat_parameters
@log_level: string: (nil)
path: string: (nil)
TEXT
      assert_equal(expected, dumped_config)
    end

    test "output complex" do
      dumped_config = capture_stdout do
        FluentPluginConfigFormatter.new(["--format=txt", "output", "complex"]).call
      end
      expected = <<TEXT
helpers: inject,compat_parameters
@log_level: string: (nil)
time_as_integer: bool: (false)
slow_flush_log_threshold: float: (20.0)
<buffer>: optional, single
 chunk_keys: array: ([])
 @type: string: ("memory")
 timekey: time: (nil)
 timekey_wait: time: (600)
 timekey_use_utc: bool: (false)
 timekey_zone: string: ("#{Time.now.strftime('%z')}")
 flush_at_shutdown: bool: (nil)
 flush_mode: enum: (:default)
 flush_interval: time: (60)
 flush_thread_count: integer: (1)
 flush_thread_interval: float: (1.0)
 flush_thread_burst_interval: float: (1.0)
 delayed_commit_timeout: time: (60)
 overflow_action: enum: (:throw_exception)
 retry_forever: bool: (false)
 retry_timeout: time: (259200)
 retry_max_times: integer: (nil)
 retry_secondary_threshold: float: (0.8)
 retry_type: enum: (:exponential_backoff)
 retry_wait: time: (1)
 retry_exponential_backoff_base: float: (2)
 retry_max_interval: time: (nil)
 retry_randomize: bool: (true)
 disable_chunk_backup: bool: (false)
<secondary>: optional, single
 @type: string: (nil)
 <buffer>: optional, single
 <secondary>: optional, single
<authentication>: required, single
 username: string: (nil)
 passowrd: string: (nil)
<parent>: optional, multiple
 <child>: optional, multiple
  names: array: (nil)
  difficulty: enum: (:normal)
TEXT
      assert_equal(expected, dumped_config)
    end
  end

  sub_test_case "markdown" do
    test "input simple" do
      dumped_config = capture_stdout do
        FluentPluginConfigFormatter.new(["--format=markdown", "input", "simple"]).call
      end
      expected = <<TEXT
## Plugin helpers

* [inject](https://docs.fluentd.org/v1.0/articles/api-plugin-helper-inject)
* [compat_parameters](https://docs.fluentd.org/v1.0/articles/api-plugin-helper-compat_parameters)

* See also: [Input Plugin Overview](https://docs.fluentd.org/v1.0/articles/input-plugin-overview)

## TestFluentPluginConfigFormatter::SimpleInput

### path (string) (required)

path to something


TEXT
      assert_equal(expected, dumped_config)
    end

    test "output complex" do
      dumped_config = capture_stdout do
        FluentPluginConfigFormatter.new(["--format=markdown", "output", "complex"]).call
      end
      expected = <<TEXT
## Plugin helpers

* [inject](https://docs.fluentd.org/v1.0/articles/api-plugin-helper-inject)
* [compat_parameters](https://docs.fluentd.org/v1.0/articles/api-plugin-helper-compat_parameters)

* See also: [Output Plugin Overview](https://docs.fluentd.org/v1.0/articles/output-plugin-overview)

## TestFluentPluginConfigFormatter::ComplexOutput


### \\<authentication\\> section (required) (single)

#### username (string) (required)

username

#### passowrd (string) (required)

password



### \\<parent\\> section (optional) (multiple)


#### \\<child\\> section (optional) (multiple)

##### names (array) (required)

names

##### difficulty (enum) (optional)

difficulty

Available values: easy, normal, hard

Default value: `normal`.




TEXT
      assert_equal(expected, dumped_config)
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
