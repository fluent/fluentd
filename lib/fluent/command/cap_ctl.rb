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
require 'fluent/log'
require 'fluent/env'
require 'fluent/capability'

module Fluent
  class CapCtl
    def prepare_option_parser
      @op = OptionParser.new

      @op.on('--clear', "Clear Fluentd Ruby capability") {|s|
        @opts[:clear_capabilities] = true
      }

      @op.on('--add [CAPABILITITY1,CAPABILITY2, ...]', "Add capabilities into Fluentd Ruby") {|s|
        @opts[:add_capabilities] = s
      }

      @op.on('--drop [CAPABILITITY1,CAPABILITY2, ...]', "Drop capabilities from Fluentd Ruby") {|s|
        @opts[:drop_capabilities] = s
      }

      @op.on('--get', "Get capabilities for Fluentd Ruby") {|s|
        @opts[:get_capabilities] = true
      }

      @op.on('-f', '--file FILE', "Specify target file to add Linux capabilities") {|s|
        @opts[:target_file] = s
      }
    end

    def usage(msg)
      puts @op.to_s
      puts "error: #{msg}" if msg
      exit 1
    end

    def initialize(argv = ARGV)
      @opts = {}
      @argv = argv

      if Fluent.linux?
        begin
          require 'capng'

          @capng = CapNG.new
        rescue LoadError
          puts "Error: capng_c is not loaded. Please install it first."
          exit 1
        end
      else
        puts "Error: This environment is not supported."
        exit 2
      end

      prepare_option_parser
    end

    def call
      parse_options!(@argv)

      target_file = if !!@opts[:target_file]
                      @opts[:target_file]
                    else
                      File.readlink("/proc/self/exe")
                    end

      if @opts[:clear_capabilities]
        clear_capabilities(@opts, target_file)
      elsif @opts[:add_capabilities]
        add_capabilities(@opts, target_file)
      elsif @opts[:drop_capabilities]
        drop_capabilities(@opts, target_file)
      end
      if @opts[:get_capabilities]
        get_capabilities(@opts, target_file)
      end
    end

    def clear_capabilities(opts, target_file)
      if !!opts[:clear_capabilities]
        @capng.clear(:caps)
        ret = @capng.apply_caps_file(target_file)
        puts "Clear capabilities #{ret ? 'done' : 'fail'}."
      end
    end

    def add_capabilities(opts, target_file)
      if add_caps = opts[:add_capabilities]
        @capng.clear(:caps)
        @capng.caps_file(target_file)
        capabilities = add_caps.split(/\s*,\s*/)
        check_capabilities(capabilities, get_valid_capabilities)
        ret = @capng.update(:add,
                            CapNG::Type::EFFECTIVE | CapNG::Type::INHERITABLE | CapNG::Type::PERMITTED,
                            capabilities)
        puts "Updating #{add_caps} #{ret ? 'done' : 'fail'}."
        ret = @capng.apply_caps_file(target_file)
        puts "Adding #{add_caps} #{ret ? 'done' : 'fail'}."
      end
    end

    def drop_capabilities(opts, target_file)
      if drop_caps = opts[:drop_capabilities]
        @capng.clear(:caps)
        @capng.caps_file(target_file)
        capabilities = drop_caps.split(/\s*,\s*/)
        check_capabilities(capabilities, get_valid_capabilities)
        ret = @capng.update(:drop,
                            CapNG::Type::EFFECTIVE | CapNG::Type::INHERITABLE | CapNG::Type::PERMITTED,
                            capabilities)
        puts "Updating #{drop_caps} #{ret ? 'done' : 'fail'}."
        @capng.apply_caps_file(target_file)
        puts "Dropping #{drop_caps} #{ret ? 'done' : 'fail'}."
      end
    end

    def get_capabilities(opts, target_file)
      if opts[:get_capabilities]
        @capng.caps_file(target_file)
        print = CapNG::Print.new
        puts "Capabilities in '#{target_file}',"
        puts "Effective:   #{print.caps_text(:buffer, :effective)}"
        puts "Inheritable: #{print.caps_text(:buffer, :inheritable)}"
        puts "Permitted:   #{print.caps_text(:buffer, :permitted)}"
      end
    end

    def get_valid_capabilities
      capabilities = []
      cap = CapNG::Capability.new
      cap.each do |_code, capability|
        capabilities << capability
      end
      capabilities
    end

    def check_capabilities(capabilities, valid_capabilities)
      capabilities.each do |capability|
        unless valid_capabilities.include?(capability)
          raise ArgumentError, "'#{capability}' is not valid capability. Valid Capabilities are:  #{valid_capabilities.join(", ")}"
        end
      end
    end

    def parse_options!(argv)
      begin
        rest = @op.parse(argv)

        if rest.length != 0
          usage nil
        end
      rescue
        usage $!.to_s
      end
    end
  end
end
