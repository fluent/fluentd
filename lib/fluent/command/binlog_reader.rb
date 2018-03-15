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

require 'optparse'
require 'msgpack'

require 'fluent/msgpack_factory'
require 'fluent/formatter'
require 'fluent/plugin'
require 'fluent/config/element'
require 'fluent/engine'
require 'fluent/version'

class FluentBinlogReader
  SUBCOMMAND = %w(cat head formats)
  HELP_TEXT = <<HELP
Usage: fluent-binlog-reader <command> [<args>]

Commands of fluent-binlog-reader:
   cat     :     Read files sequentially, writing them to standard output.
   head    :     Display the beginning of a text file.
   formats :     Display plugins that you can use.

See 'fluent-binlog-reader <command> --help' for more information on a specific command.
HELP

  def initialize(argv = ARGV)
    @argv = argv
  end

  def call
    command_class = BinlogReaderCommand.const_get(command)
    command_class.new(@argv).call
  end

  private

  def command
    command = @argv.shift
    if command
      if command == '--version'
        puts "#{File.basename($PROGRAM_NAME)} #{Fluent::VERSION}"
        exit 0
      elsif !SUBCOMMAND.include?(command)
        usage "'#{command}' is not supported: Required subcommand : #{SUBCOMMAND.join(' | ')}"
      end
    else
      usage "Required subcommand : #{SUBCOMMAND.join(' | ')}"
    end

    command.split('_').map(&:capitalize).join('')
  end

  def usage(msg = nil)
    puts HELP_TEXT
    puts "Error: #{msg}" if msg
    exit 1
  end
end

module BinlogReaderCommand
  class Base
    def initialize(argv = ARGV)
      @argv = argv

      @options = { plugin: [] }
      @opt_parser = OptionParser.new do |opt|
        opt.version = Fluent::VERSION
        opt.separator 'Options:'

        opt.on('-p DIR', '--plugin', 'add library directory path') do |v|
          @options[:plugin] << v
        end
      end
    end

    def call
      raise NotImplementedError, 'BUG: command  MUST implement this method'
    end

    private

    def usage(msg = nil)
      puts @opt_parser.to_s
      puts "Error: #{msg}" if msg
      exit 1
    end

    def parse_options!
      @opt_parser.parse!(@argv)

      unless @options[:plugin].empty?
        if dir = @options[:plugin].find { |d| !Dir.exist?(d) }
          usage "Directory #{dir} doesn't exist"
        else
          @options[:plugin].each do |d|
            Fluent::Plugin.add_plugin_dir(d)
          end
        end
      end
    rescue => e
      usage e
    end
  end

  module Formattable
    DEFAULT_OPTIONS = {
      format: :out_file
    }

    def initialize(argv = ARGV)
      super
      @options.merge!(DEFAULT_OPTIONS)
      configure_option_parser
    end

    private

    def configure_option_parser
      @options.merge!(config_params: {})

      @opt_parser.banner = "Usage: fluent-binlog-reader #{self.class.to_s.split('::').last.downcase} [options] file"

      @opt_parser.on('-f TYPE', '--format', 'configure output format') do |v|
        @options[:format] = v.to_sym
      end

      @opt_parser.on('-e KEY=VALUE', 'configure formatter config params') do |v|
        key, value = v.split('=')
        usage "#{v} is invalid. valid format is like `key=value`" unless value
        @options[:config_params].merge!(key => value)
      end
    end

    def lookup_formatter(format, params)
      conf = Fluent::Config::Element.new('ROOT', '', params, [])
      formatter = Fluent::Plugin.new_formatter(format)

      if formatter.respond_to?(:configure)
        formatter.configure(conf)
      end
      formatter
    rescue => e
      usage e
    end
  end

  class Head < Base
    include Formattable

    DEFAULT_HEAD_OPTIONS = {
      count: 5
    }

    def initialize(argv = ARGV)
      super
      @options.merge!(default_options)
      parse_options!
    end

    def call
      @formatter = lookup_formatter(@options[:format], @options[:config_params])

      File.open(@path, 'rb') do |io|
        i = 1
        Fluent::MessagePackFactory.unpacker(io).each do |(time, record)|
          print @formatter.format(@path, time, record) # path is used for tag
          break if @options[:count] && i == @options[:count]
          i += 1
        end
      end
    end

    private

    def default_options
      DEFAULT_HEAD_OPTIONS
    end

    def parse_options!
      @opt_parser.on('-n COUNT', 'Set the number of lines to display') do |v|
        @options[:count] = v.to_i
        usage "illegal line count -- #{@options[:count]}" if @options[:count] < 1
      end

      super

      usage 'Path is required' if @argv.empty?
      @path = @argv.first
      usage "#{@path} is not found" unless File.exist?(@path)
    end
  end

  class Cat < Head
    DEFAULT_CAT_OPTIONS = {
      count: nil                # Overwrite DEFAULT_HEAD_OPTIONS[:count]
    }

    def default_options
      DEFAULT_CAT_OPTIONS
    end
  end

  class Formats < Base
    def initialize(argv = ARGV)
      super
      parse_options!
    end

    def call
      prefix = Fluent::Plugin::FORMATTER_REGISTRY.dir_search_prefix || 'formatter_'

      plugin_dirs = @options[:plugin]
      unless plugin_dirs.empty?
        plugin_dirs.each do |d|
          Dir.glob("#{d}/#{prefix}*.rb").each do |path|
            require File.absolute_path(path)
          end
        end
      end

      $LOAD_PATH.map do |lp|
        Dir.glob("#{lp}/#{prefix}*.rb").each do |path|
          require path
        end
      end

      puts Fluent::Plugin::FORMATTER_REGISTRY.map.keys
    end
  end
end
