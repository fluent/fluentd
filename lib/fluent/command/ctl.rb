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
require 'fluent/env'
require 'fluent/version'
require 'win32/event' if Fluent.windows?

module Fluent
  class Ctl
    DEFAULT_OPTIONS = {}
    UNIX_SIGNAL_MAP = {
      shutdown: :TERM,
      restart: :HUP,
      flush: :USR1,
      reload: :USR2,
    }
    WINDOWS_EVENT_MAP = {
      shutdown: "",
      restart: "HUP",
      flush: "USR1",
      reload: "USR2",
    }
    if Fluent.windows?
      COMMAND_MAP = WINDOWS_EVENT_MAP
    else
      COMMAND_MAP = UNIX_SIGNAL_MAP
    end

    def initialize(argv = ARGV)
      @argv = argv
      @options = {}
      @opt_parser = OptionParser.new
      configure_option_parser
      @options.merge!(DEFAULT_OPTIONS)
      parse_options!
    end

    def help_text
      text = "\n"
      if Fluent.windows?
        text << "Usage: #{$PROGRAM_NAME} COMMAND PID_OR_SIGNAME\n"
      else
        text << "Usage: #{$PROGRAM_NAME} COMMAND PID\n"
      end
      text << "\n"
      text << "Commands: \n"
      COMMAND_MAP.each do |key, value|
        text << "  #{key}\n"
      end
      text
    end

    def usage(msg = nil)
      puts help_text
      if msg
        puts
        puts "Error: #{msg}"
      end
      exit 1
    end

    def call
      if Fluent.windows?
        call_windows_event(@command, @pid_or_signame)
      else
        call_signal(@command, @pid_or_signame)
      end
    end

    private

    def call_signal(command, pid)
      signal = COMMAND_MAP[command.to_sym]
      Process.kill(signal, pid.to_i)
    end

    def call_windows_event(command, pid_or_signame)
      if pid_or_signame =~ /^[0-9]+$/
        prefix = "fluentd_#{pid_or_signame}"
      else
        prefix = pid_or_signame
      end
      event_name = COMMAND_MAP[command.to_sym]
      suffix = event_name.empty? ? "" : "_#{event_name}"

      begin
        event = Win32::Event.open("#{prefix}#{suffix}")
        event.set
        event.close
      rescue Errno::ENOENT => e
        puts "Error: Cannot find the fluentd process with the event name: \"#{prefix}\""
      end
    end

    def configure_option_parser
      @opt_parser.banner = help_text
      @opt_parser.version = Fluent::VERSION
    end

    def parse_options!
      @opt_parser.parse!(@argv)

      @command = @argv[0]
      @pid_or_signame = @argv[1]

      usage("Command isn't specified!") if @command.nil? || @command.empty?
      usage("Unknown command: #{@command}") unless COMMAND_MAP.has_key?(@command.to_sym)

      if Fluent.windows?
        usage("PID or SIGNAME isn't specified!") if @pid_or_signame.nil? || @pid_or_signame.empty?
      else
        usage("PID isn't specified!") if @pid_or_signame.nil? || @pid_or_signame.empty?
        usage("Invalid PID: #{pid}") unless @pid_or_signame =~ /^[0-9]+$/
      end
    end
  end
end

