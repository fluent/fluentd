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

    def lookup_formatter
      formatter = Fluent::Plugin.new_formatter(@format)
      conf = Fluent::Config::Element.new('ROOT', '', {}, [])

      if formatter.respond_to?(:configure)
        formatter.configure(conf)
      end
      formatter
    end
  end

  class Head < Base
    DEFAULT_OPTIONS = { number: 5 }

    def initialize(argv, format)
      super
      @options = DEFAULT_OPTIONS
      @path = option_parser.parse(@argv).first
      # validate
    end

    def call
      @formatter = lookup_formatter

      io =  File.open(@path, 'r')
      i = 0
      ret = []
      unpacker(io).each do |(time, record)|
        break if i == @options[:number]
        i += 1
        puts @formatter.format(@path, time, record) # tag is use for tag
      end
      puts ret
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
      puts Fluent::Plugin::FORMATTER_REGISTRY.map.keys
    end
  end
end
