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

require 'fluent/msgpack_factory'
require 'fluent/formatter'
require 'fluent/plugin'
require 'msgpack'
require 'fluent/config/element'

class FluentUnpacker

  SUBCOMMAND = %(cat tail head formats)
  DEFAULT_OPTIONS = { format: :json }

  def initialize(argv = ARGV)
    @argv = argv
    @options = DEFAULT_OPTIONS
    @path = nil
    @subcommand = nil
    @sub_options = nil

    parse!
  end

  def call
    Fluent::Plugin.add_plugin_dir(@options[:plugins])

    subcommand_class = SubCommand.const_get(@subcommand.split('_').map(&:capitalize).join('')) # todo handle underscore
    subcommand_class.new(@sub_options, @format).call
  end

  private

  def parse!
    ret = option_parser.order(ARGV)
    subcommand, *sub_options = ret    # multiple file?
    case
    when !subcommand
      usage "Required subcommand ${SUBCOMMAND}"
    else
      @subcommand = subcommand
      @sub_options = sub_options
      @format = @options.delete(:format)
    end
  rescue
    usage $!.to_s
  end

  def usage(msg = nil)
    puts option_parser.to_s
    puts "Error: #{msg}" if msg
    exit 1
  end

  def option_parser
    @option_parser ||= OptionParser.new do |opt|
      opt.banner = 'Usage: fluent-unpacker subcoomand [options]'
      opt.separator 'Options:'

      # TOFIX
      opt.on('-f TYPE', '--format', "configure output format: ") do |v|
        @options[:format] = v.to_sym
      end

      opt.on('-p DIR', '--plugins', "pdir: ") do |v|
        @options[:plugins] = v
      end
    end
  end
end

module SubCommand
  class Base
    def initialize(argv = ARGV, format)
      @argv = argv
      @format = format
    end

    def configure
      raise NotImplementedError, "Must Implement this method"
    end

    def call
      raise NotImplementedError, "Must Implement this method"
    end

    def unpacker(io)
      MessagePack::Unpacker.new(io)
    end

    def usage(msg = nil)
      puts option_parser.to_s
      puts "Error: #{msg}" if msg
      exit 1
    end

    def option_parser
      raise NotImplementedError, "Must Implement this method"
    end

    def lookup_formatter(conf)
      formatter = Fluent::Plugin.new_formatter(@format)

      if formatter.respond_to?(:configure)
        formatter.configure(conf)
      end
      formatter
    end
  end

  class Head < Base
    DEFAULT_OPTIONS = { number: 5 }

    def initialize(argv, format)
      if i = argv.index("--")
        @v = argv[i+1..-1]
        argv = argv[0...i]
      end

      super
      @options = DEFAULT_OPTIONS
      ret = option_parser.parse(@argv)
      @path = ret.first

      @v = @v.reduce({}) { |acc, e|
        k, v = e[1..-1].split("=")
        acc.merge(k => v)
      }

      # validate
    end

    def call
      conf = Fluent::Config::Element.new('ROOT', '', @v, [])
      @formatter = lookup_formatter(conf)

      io = File.open(@path, 'r')
      i = 0
      unpacker(io).each do |(time, record)|
        break if i == @options[:number]
        i += 1
        puts @formatter.format(@path, time, record) # tag is use for tag
      end
    end

    def option_parser
      @option_parser ||= OptionParser.new do |opt|
        opt.banner = 'Usage: fluent-unpacker head [options] files'
        opt.separator 'Options:'

        opt.on('-n COUNT', "tail liek -n") do |v|
          @options[:number] = v.to_i
        end
      end
    end
  end

  class Tail < Base
  end

  class Formats < Base
    def call
      pf = Fluent::Plugin::FORMATTER_REGISTRY.paths.last

      if prefix = Fluent::Plugin::FORMATTER_REGISTRY.dir_search_prefix
        Dir.glob("#{pf}/#{prefix}*").each do |e|
          require File.absolute_path(e)
        end

        $LOAD_PATH.map do |lp|
          Dir.glob("#{lp}/#{prefix}*").each do |e|
            require e
          end
        end

        specs = Gem::Specification.flat_map { |spec| spec.lib_files }.select do |e|
          e.include?(prefix)
        end
        specs.each do |e|
          require File.absolute_path(e)
        end
      end

      puts Fluent::Plugin::FORMATTER_REGISTRY.map.keys
    end
  end
end
