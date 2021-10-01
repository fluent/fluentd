#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require "optparse"
require "pathname"
require "fileutils"
require "erb"
require "open-uri"

require "fluent/env"
require "fluent/registry"
require 'fluent/version'

class FluentPluginGenerator
  attr_reader :type, :name
  attr_reader :license_name

  SUPPORTED_TYPES = ["input", "output", "filter", "parser", "formatter", "storage"]

  def initialize(argv = ARGV)
    @argv = argv
    @parser = prepare_parser

    @license_name = "Apache-2.0"
    @overwrite_all = false
  end

  def call
    parse_options!
    FileUtils.mkdir_p(gem_name, mode: Fluent::DEFAULT_DIR_PERMISSION)
    Dir.chdir(gem_name) do
      copy_license
      template_directory.find do |path|
        next if path.directory?
        dest_dir = path.dirname.sub(/\A#{Regexp.quote(template_directory.to_s)}\/?/, "")
        dest_file = dest_filename(path)
        if path.extname == ".erb"
          if path.fnmatch?("*/plugin/*")
            next unless path.basename.fnmatch?("*#{type}*")
          end
          template(path, dest_dir + dest_file)
        else
          file(path, dest_dir + dest_file)
        end
      end
      pid = spawn("git", "init", ".")
      Process.wait(pid)
    end
  end

  private

  def template_directory
    (Pathname(__dir__) + "../../../templates/new_gem").realpath
  end

  def template_file(filename)
    template_directory + filename
  end

  def template(source, dest)
    dest.dirname.mkpath
    contents =
      if ERB.instance_method(:initialize).parameters.assoc(:key) # Ruby 2.6+
        ERB.new(source.read, trim_mode: "-")
      else
        ERB.new(source.read, nil, "-")
      end.result(binding)
    label = create_label(dest, contents)
    puts "\t#{label} #{dest}"
    if label == "conflict"
      return unless overwrite?(dest)
    end
    File.write(dest, contents)
  end

  def file(source, dest)
    label = create_label(dest, source.read)
    puts "\t#{label} #{dest}"
    if label == "conflict"
      return unless overwrite?(dest)
    end
    FileUtils.cp(source, dest)
  end

  def prepare_parser
    @parser = OptionParser.new
    @parser.version = Fluent::VERSION
    @parser.banner = <<BANNER
Usage: fluent-plugin-generate [options] <type> <name>

Generate a project skeleton for creating a Fluentd plugin

Arguments:
\ttype: #{SUPPORTED_TYPES.join(",")}
\tname: Your plugin name (fluent-plugin- prefix will be added to <name>)

Options:
BANNER

    @parser.on("--[no-]license=NAME", "Specify license name (default: Apache-2.0)") do |v|
      @license_name = v || "no-license"
    end
    @parser
  end

  def parse_options!
    @parser.parse!(@argv)
    unless @argv.size == 2
      raise ArgumentError, "Missing arguments"
    end
    @type, @name = @argv
  rescue => e
    usage("#{e.class}:#{e.message}")
  end

  def usage(message = "")
    puts message
    puts
    puts @parser.help
    exit(false)
  end

  def user_name
    v = `git config --get user.name`.chomp
    v.empty? ? "TODO: Write your name" : v
  end

  def user_email
    v = `git config --get user.email`.chomp
    v.empty? ? "TODO: Write your email" : v
  end

  def gem_name
    "fluent-plugin-#{dash_name}"
  end

  def plugin_name
    underscore_name
  end

  def gem_file_path
    File.expand_path(File.join(File.dirname(__FILE__),
                               "../../../",
                               "Gemfile"))
  end

  def lock_file_path
    File.expand_path(File.join(File.dirname(__FILE__),
                               "../../../",
                               "Gemfile.lock"))
  end

  def locked_gem_version(gem_name)
    if File.exist?(lock_file_path)
      d = Bundler::Definition.build(gem_file_path, lock_file_path, false)
      d.locked_gems.dependencies[gem_name].requirement.requirements.first.last.version
    else
      # fallback even though Fluentd is installed without bundler
      Gem::Specification.find_by_name(gem_name).version.version
    end
  end

  def rake_version
    locked_gem_version("rake")
  end

  def test_unit_version
    locked_gem_version("test-unit")
  end

  def bundler_version
    if File.exist?(lock_file_path)
      d = Bundler::Definition.build(gem_file_path, lock_file_path, false)
      d.locked_gems.bundler_version.version
    else
      # fallback even though Fluentd is installed without bundler
      Gem::Specification.find_by_name("bundler").version.version
    end
  end

  def class_name
    "#{capitalized_name}#{type.capitalize}"
  end

  def plugin_filename
    case type
    when "input"
      "in_#{underscore_name}.rb"
    when "output"
      "out_#{underscore_name}.rb"
    else
      "#{type}_#{underscore_name}.rb"
    end
  end

  def test_filename
    case type
    when "input"
      "test_in_#{underscore_name}.rb"
    when "output"
      "test_out_#{underscore_name}.rb"
    else
      "test_#{type}_#{underscore_name}.rb"
    end
  end

  def dest_filename(path)
    case path.to_s
    when %r!\.gemspec!
      "#{gem_name}.gemspec"
    when %r!lib/fluent/plugin!
      plugin_filename
    when %r!test/plugin!
      test_filename
    else
      path.basename.sub_ext("")
    end
  end

  def capitalized_name
    @capitalized_name ||= name.split(/[-_]/).map(&:capitalize).join
  end

  def underscore_name
    @underscore_name ||= name.tr("-", "_")
  end

  def dash_name
    @dash_name ||= name.tr("_", "-")
  end

  def preamble
    @license.preamble(user_name)
  end

  def copy_license
    # in gem_name directory
    return unless license_name
    puts "License: #{license_name}"
    license_class = self.class.lookup_license(license_name)
    @license = license_class.new
    Pathname("LICENSE").write(@license.text) unless @license.text.empty?
  rescue Fluent::ConfigError
    usage("Unknown license: #{license_name}")
  rescue => ex
    usage("#{ex.class}: #{ex.message}")
  end

  def create_label(dest, contents)
    if dest.exist?
      if dest.read == contents
        "identical"
      else
        "conflict"
      end
    else
      "create"
    end
  end

  def overwrite?(dest)
    return true if @overwrite_all
    loop do
      print "Overwrite #{dest}? (enter \"h\" for help) [Ynaqh]"
      answer = $stdin.gets.chomp
      return true if /\Ay\z/i =~ answer || answer.empty?
      case answer
      when "n"
        return false
      when "a"
        @overwrite_all = true
        return true
      when "q"
        exit
      when "h"
        puts <<HELP
\tY - yes, overwrite
\tn - no, do not overwrite
\ta - all, overwrite this and all others
\tq - quit, abort
\th - help, show this help
HELP
      end
      puts "Retrying..."
    end
  end

  class NoLicense
    attr_reader :name, :full_name, :text

    def initialize
      @name = ""
      @full_name = ""
      @text = ""
    end

    def preamble(usename)
      ""
    end
  end

  class ApacheLicense
    LICENSE_URL = "http://www.apache.org/licenses/LICENSE-2.0.txt"

    attr_reader :text

    def initialize
      @text = ""
      @preamble_source = ""
      @preamble = nil
      uri = URI.parse(LICENSE_URL)
      uri.open do |io|
        @text = io.read
      end
      @preamble_source = @text[/^(\s*Copyright.+)/m, 1]
    end

    def name
      "Apache-2.0"
    end

    def full_name
      "Apache License, Version 2.0"
    end

    def preamble(user_name)
      @preamble ||= @preamble_source.dup.tap do |source|
        source.gsub!(/\[yyyy\]/, "#{Date.today.year}-")
        source.gsub!(/\[name of copyright owner\]/, user_name)
        source.gsub!(/^ {2}|^$/, "#")
        source.chomp!
      end
    end
  end

  LICENSE_REGISTRY = Fluent::Registry.new(:license, "")

  def self.register_license(license, klass)
    LICENSE_REGISTRY.register(license, klass)
  end

  def self.lookup_license(license)
    LICENSE_REGISTRY.lookup(license)
  end

  {
    "no-license" => NoLicense,
    "Apache-2.0" => ApacheLicense
  }.each do |license, klass|
    register_license(license, klass)
  end
end
