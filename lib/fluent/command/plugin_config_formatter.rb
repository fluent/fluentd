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

require "erb"
require "optparse"
require "pathname"
require "fluent/plugin"
require "fluent/env"
require "fluent/engine"
require "fluent/system_config"
require "fluent/config/element"
require 'fluent/version'

class FluentPluginConfigFormatter

  AVAILABLE_FORMATS = [:markdown, :txt, :json]
  SUPPORTED_TYPES = [
    "input", "output", "filter",
    "buffer", "parser", "formatter", "storage"
  ]

  DOCS_BASE_URL = "https://docs.fluentd.org/v1.0/articles/"

  def initialize(argv = ARGV)
    @argv = argv

    @compact = false
    @format = :markdown
    @verbose = false
    @libs = []
    @plugin_dirs = []
    @options = {}

    prepare_option_parser
  end

  def call
    parse_options!
    init_libraries
    @plugin = Fluent::Plugin.__send__("new_#{@plugin_type}", @plugin_name)
    dumped_config = {}
    if @plugin.class.respond_to?(:plugin_helpers)
      dumped_config[:plugin_helpers] = @plugin.class.plugin_helpers
    end
    @plugin.class.ancestors.reverse_each do |plugin_class|
      next unless plugin_class.respond_to?(:dump_config_definition)
      unless @verbose
        next if plugin_class.name =~ /::PluginHelper::/
      end
      dumped_config_definition = plugin_class.dump_config_definition
      dumped_config[plugin_class.name] = dumped_config_definition unless dumped_config_definition.empty?
    end
    case @format
    when :txt
      puts dump_txt(dumped_config)
    when :markdown
      puts dump_markdown(dumped_config)
    when :json
      puts dump_json(dumped_config)
    end
  end

  private

  def dump_txt(dumped_config)
    dumped = ""
    plugin_helpers = dumped_config.delete(:plugin_helpers)
    if plugin_helpers && !plugin_helpers.empty?
      dumped << "helpers: #{plugin_helpers.join(',')}\n"
    end
    if @verbose
      dumped_config.each do |name, config|
        dumped << "#{name}\n"
        dumped << dump_section_txt(config)
      end
    else
      configs = dumped_config.values
      root_section = configs.shift
      configs.each do |config|
        root_section.update(config)
      end
      dumped << dump_section_txt(root_section)
    end
    dumped
  end

  def dump_section_txt(base_section, level = 0)
    dumped = ""
    indent = " " * level
    if base_section[:section]
      sections = []
      params = base_section
    else
      sections, params = base_section.partition {|_name, value| value[:section] }
    end
    params.each do |name, config|
      next if name == :section
      dumped << "#{indent}#{name}: #{config[:type]}: (#{config[:default].inspect})"
      dumped << " # #{config[:description]}" if config.key?(:description)
      dumped << "\n"
    end
    sections.each do |section_name, sub_section|
      required = sub_section.delete(:required)
      multi = sub_section.delete(:multi)
      alias_name = sub_section.delete(:alias)
      required_label = required ? "required" : "optional"
      multi_label = multi ? "multiple" : "single"
      alias_label = "alias: #{alias_name}"
      dumped << "#{indent}<#{section_name}>: #{required_label}, #{multi_label}"
      if alias_name
        dumped << ", #{alias_label}\n"
      else
        dumped << "\n"
      end
      sub_section.delete(:section)
      dumped << dump_section_txt(sub_section, level + 1)
    end
    dumped
  end

  def dump_markdown(dumped_config)
    dumped = ""
    plugin_helpers = dumped_config.delete(:plugin_helpers)
    if plugin_helpers && !plugin_helpers.empty?
      dumped = "## Plugin helpers\n\n"
      plugin_helpers.each do |plugin_helper|
        dumped << "* #{plugin_helper_markdown_link(plugin_helper)}\n"
      end
      dumped << "\n"
    end
    dumped_config.each do |name, config|
      if name == @plugin.class.name
        dumped << "## #{name}\n\n"
        dumped << dump_section_markdown(config)
      else
        dumped << "* See also: #{plugin_overview_markdown_link(name)}\n\n"
      end
    end
    dumped
  end

  def dump_section_markdown(base_section, level = 0)
    dumped = ""
    if base_section[:section]
      sections = []
      params = base_section
    else
      sections, params = base_section.partition {|_name, value| value[:section] }
    end
    params.each do |name, config|
      next if name == :section
      template_name = @compact ? "param.md-compact.erb" : "param.md.erb"
      dumped << ERB.new(template_path(template_name).read, nil, "-").result(binding)
    end
    dumped << "\n"
    sections.each do |section_name, sub_section|
      required = sub_section.delete(:required)
      multi = sub_section.delete(:multi)
      alias_name = sub_section.delete(:alias)
      sub_section.delete(:section)
      dumped << ERB.new(template_path("section.md.erb").read, nil, "-").result(binding)
    end
    dumped
  end

  def dump_json(dumped_config)
    if @compact
      JSON.generate(dumped_config)
    else
      JSON.pretty_generate(dumped_config)
    end
  end

  def plugin_helper_url(plugin_helper)
    "#{DOCS_BASE_URL}api-plugin-helper-#{plugin_helper}"
  end

  def plugin_helper_markdown_link(plugin_helper)
    "[#{plugin_helper}](#{plugin_helper_url(plugin_helper)})"
  end

  def plugin_overview_url(class_name)
    plugin_type = class_name.slice(/::(\w+)\z/, 1).downcase
    "#{DOCS_BASE_URL}#{plugin_type}-plugin-overview"
  end

  def plugin_overview_markdown_link(class_name)
    plugin_type = class_name.slice(/::(\w+)\z/, 1)
    "[#{plugin_type} Plugin Overview](#{plugin_overview_url(class_name)})"
  end

  def usage(message = nil)
    puts @parser.to_s
    puts
    puts "Error: #{message}" if message
    exit(false)
  end

  def prepare_option_parser
    @parser = OptionParser.new
    @parser.version = Fluent::VERSION
    @parser.banner = <<BANNER
Usage: #{$0} [options] <type> <name>

Output plugin config definitions

Arguments:
\ttype: #{SUPPORTED_TYPES.join(",")}
\tname: registered plugin name

Options:
BANNER
    @parser.on("--verbose", "Be verbose") do
      @verbose = true
    end
    @parser.on("-c", "--compact", "Compact output") do
      @compact = true
    end
    @parser.on("-f", "--format=FORMAT", "Specify format. (#{AVAILABLE_FORMATS.join(',')})") do |s|
      format = s.to_sym
      usage("Unsupported format: #{s}") unless AVAILABLE_FORMATS.include?(format)
      @format = format
    end
    @parser.on("-I PATH", "Add PATH to $LOAD_PATH") do |s|
      $LOAD_PATH.unshift(s)
    end
    @parser.on("-r NAME", "Load library") do |s|
      @libs << s
    end
    @parser.on("-p", "--plugin=DIR", "Add plugin directory") do |s|
      @plugin_dirs << s
    end
  end

  def parse_options!
    @parser.parse!(@argv)

    raise "Must specify plugin type and name" unless @argv.size == 2

    @plugin_type, @plugin_name = @argv
    @options = {
      compact: @compact,
      format: @format,
      verbose: @verbose,
    }
  rescue => e
    usage(e)
  end

  def init_libraries
    @libs.each do |lib|
      require lib
    end

    @plugin_dirs.each do |dir|
      if Dir.exist?(dir)
        dir = File.expand_path(dir)
        Fluent::Plugin.add_plugin_dir(dir)
      end
    end
  end

  def template_path(name)
    (Pathname(__dir__) + "../../../templates/plugin_config_formatter/#{name}").realpath
  end
end
