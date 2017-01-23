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
require "fluent/plugin"
require "fluent/env"
require "fluent/engine"
require "fluent/system_config"
require "fluent/config/element"

class FluentPluginConfigFormatter

  AVAILABLE_FORMATS = [:txt, :markdown]

  def initialize(argv = ARGV)
    @argv = argv

    @all = false
    @format = :markdown
    @libs = []
    @plugin_dirs = []

    prepare_option_parser
  end

  def call
    parse_options!
    init_engine
    plugin = Fluent::Plugin.__send__("new_#{@plugin_type}", @plugin_name)
    if @format == :markdown
      helpers = "### Plugin_helpers\n\n"
      plugin.class.plugin_helpers.each do |helper|
        helpers << "* #{helper}\n"
      end
      puts helpers
    end
    plugin.class.ancestors.reverse_each do |plugin_class|
      next unless plugin_class.respond_to?(:dump)
      unless @verbose
        next if plugin_class.name =~ /::PluginHelper::/
      end
      puts plugin_class.dump(0, @format)
    end
  end

  private

  def usage(message = nil)
    puts @paser.to_s
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
    @parser.on("-f", "--format=FORMAT", "Specify format") do |s|
      @format = s.to_sym
    end
    @parser.on("-r NAME", "Add library path") do |s|
      @libs << s
    end
    @parser.on("-p", "--plugin=DIR", "Add plugin directory") do |s|
      @plugin_dirs << s
    end
    @parser.on("-a", "--all", "Show all") do
      @all = true
    end
  end

  def parse_options!
    @parser.parse!(@argv)

    raise "Must specify plugin type and name" unless @argv.size == 2

    @plugin_type, @plugin_name = @argv
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
end
