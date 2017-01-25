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

class FluentPluginConfigFormatter

  AVAILABLE_FORMATS = [:txt, :markdown, :json]

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
    init_engine
    @plugin = Fluent::Plugin.__send__("new_#{@plugin_type}", @plugin_name)
    dumped_config = {}
    dumped_config[:plugin_helpers] = @plugin.class.plugin_helpers
    @plugin.class.ancestors.reverse_each do |plugin_class|
      next unless plugin_class.respond_to?(:dump)
      next if plugin_class == Fluent::Plugin::Base
      unless @verbose
        next if plugin_class.name =~ /::PluginHelper::/
      end
      dumped_config[plugin_class.name] = plugin_class.dump
    end
    puts __send__("dump_#{@format}", dumped_config)
  end

  private

  def dump_txt(dumped_config)
    dumped = ""
    plugin_helpers = dumped_config.delete(:plugin_helpers)
    dumped << "helpers: #{plugin_helpers.join(',')}\n" if plugin_helpers
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
      dumped << "#{dump_section_txt(sub_section, level + 1)}"
    end
    dumped
  end

  def dump_markdown(dumped_config)
    dumped = ""
    plugin_helpers = dumped_config.delete(:plugin_helpers)
    if plugin_helpers
      dumped = "## Plugin helpers\n\n"
      plugin_helpers.each do |plugin_helper|
        dumped << "* #{plugin_helper}\n"
      end
      dumped << "\n"
    end
    configs = dumped_config.values
    root_section = configs.shift
    dumped << "## #{@plugin.class.name}\n\n"
    configs.each do |config|
      root_section.update(config)
    end
    dumped << dump_section_markdown(root_section)
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

  def usage(message = nil)
    puts @parser.to_s
    puts "Error: #{message}" if message
    exit(false)
  end

  def prepare_option_parser
    @parser = OptionParser.new
    @parser.banner = <<BANNER
Usage: #{$0} [options] <type> <name>
BANNER
    @parser.on("--verbose", "Be verbose") do
      @verbose = true
    end
    @parser.on("-c", "--compact", "Compact output") do
      @compact = true
    end
    @parser.on("-f", "--format=FORMAT", "Specify format") do |s|
      @format = s.to_sym
    end
    @parser.on("-r NAME", "Add library path") do |s|
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

  def init_engine
    system_config = Fluent::SystemConfig.new
    Fluent::Engine.init(system_config)

    @libs.each do |lib|
      require lib
    end

    @plugin_dirs.each do |dir|
      if Dir.exist?(dir)
        dir = File.expand_path(dir)
        Fluent::Engine.add_plugin_dir(dir)
      end
    end
  end

  def template_path(name)
    Pathname(__dir__) + "./templates/plugin_config_formatter/#{name}"
  end
end
