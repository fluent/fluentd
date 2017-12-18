require_relative '../helper'

require 'pathname'
require 'fluent/command/plugin_generator'

class TestFluentPluginGenerator < Test::Unit::TestCase
  TEMP_DIR = "tmp/plugin_generator"
  setup do
    FileUtils.mkdir_p(TEMP_DIR)
    @pwd = Dir.pwd
    Dir.chdir(TEMP_DIR)
  end

  teardown do
    Dir.chdir(@pwd)
    FileUtils.rm_rf(TEMP_DIR)
  end

  sub_test_case "generate plugin" do
    data(input: ["input", "in"],
         output: ["output", "out"],
         filter: ["filter", "filter"],
         parser: ["parser", "parser"],
         formatter: ["formatter", "formatter"])
    test "generate plugin" do |(type, part)|
      capture_stdout do
        FluentPluginGenerator.new([type, "fake"]).call
      end
      plugin_base_dir = Pathname("fluent-plugin-fake")
      assert { plugin_base_dir.directory? }
      expected = [
        "fluent-plugin-fake",
        "fluent-plugin-fake/Gemfile",
        "fluent-plugin-fake/LICENSE",
        "fluent-plugin-fake/README.md",
        "fluent-plugin-fake/Rakefile",
        "fluent-plugin-fake/fluent-plugin-fake.gemspec",
        "fluent-plugin-fake/lib",
        "fluent-plugin-fake/lib/fluent",
        "fluent-plugin-fake/lib/fluent/plugin",
        "fluent-plugin-fake/lib/fluent/plugin/#{part}_fake.rb",
        "fluent-plugin-fake/test",
        "fluent-plugin-fake/test/helper.rb",
        "fluent-plugin-fake/test/plugin",
        "fluent-plugin-fake/test/plugin/test_#{part}_fake.rb",
      ]
      actual = plugin_base_dir.find.reject {|f| f.fnmatch("*/.git*") }.map(&:to_s).sort
      assert_equal(expected, actual)
    end

    test "no license" do
      capture_stdout do
        FluentPluginGenerator.new(["--no-license", "filter", "fake"]).call
      end
      assert { !Pathname("fluent-plugin-fake/LICENSE").exist? }
      assert { Pathname("fluent-plugin-fake/Gemfile").exist? }
    end

    test "unknown license" do
      out = capture_stdout do
        assert_raise(SystemExit) do
          FluentPluginGenerator.new(["--license=unknown", "filter", "fake"]).call
        end
      end
      assert { out.lines.include?("License: unknown\n") }
    end
  end

  sub_test_case "unify plugin name" do
    data("word" => ["fake", "fake"],
         "underscore" => ["rewrite_tag_filter", "rewrite_tag_filter"],
         "dash" => ["rewrite-tag-filter", "rewrite_tag_filter"])
    test "plugin_name" do |(name, plugin_name)|
      generator = FluentPluginGenerator.new(["filter", name])
      capture_stdout do
        generator.call
      end
      assert_equal(plugin_name, generator.__send__(:plugin_name))
    end

    data("word" => ["fake", "fluent-plugin-fake"],
         "underscore" => ["rewrite_tag_filter", "fluent-plugin-rewrite-tag-filter"],
         "dash" => ["rewrite-tag-filter", "fluent-plugin-rewrite-tag-filter"])
    test "gem_name" do |(name, gem_name)|
      generator = FluentPluginGenerator.new(["output", name])
      capture_stdout do
        generator.call
      end
      assert_equal(gem_name, generator.__send__(:gem_name))
    end
  end
end
