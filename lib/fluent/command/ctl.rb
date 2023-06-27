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
if Fluent.windows?
  require 'win32/event'
  require 'win32/service'
end

module Fluent
  class Ctl
    DEFAULT_OPTIONS = {}

    if Fluent.windows?
      include Windows::ServiceConstants
      include Windows::ServiceStructs
      include Windows::ServiceFunctions

      COMMAND_MAP = {
        shutdown: "",
        restart: "HUP",
        flush: "USR1",
        reload: "USR2",
        dump: "CONT",
      }
      WINSVC_CONTROL_CODE_MAP = {
        shutdown: SERVICE_CONTROL_STOP,
        # 128 - 255: user-defined control code
        # See https://docs.microsoft.com/en-us/windows/win32/api/winsvc/nf-winsvc-controlservice
        restart: 128,
        flush: 129,
        reload: SERVICE_CONTROL_PARAMCHANGE,
        dump: 130,
      }
    else
      COMMAND_MAP = {
        shutdown: :TERM,
        restart: :HUP,
        flush: :USR1,
        reload: :USR2,
        dump: :CONT,
      }
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
        text << "Usage: #{$PROGRAM_NAME} COMMAND [PID_OR_SVCNAME]\n"
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
        if /^[0-9]+$/.match?(@pid_or_svcname)
          # Use as PID
          return call_windows_event(@command, "fluentd_#{@pid_or_svcname}")
        end

        unless call_winsvc_control_code(@command, @pid_or_svcname)
          puts "Cannot send control code to #{@pid_or_svcname} service, try to send an event with this name ..."
          call_windows_event(@command, @pid_or_svcname)
        end
      else
        call_signal(@command, @pid_or_svcname)
      end
    end

    private

    def call_signal(command, pid)
      signal = COMMAND_MAP[command.to_sym]
      Process.kill(signal, pid.to_i)
    end

    def call_winsvc_control_code(command, pid_or_svcname)
      status = SERVICE_STATUS.new

      begin
        handle_scm = OpenSCManager(nil, nil, SC_MANAGER_CONNECT)
        FFI.raise_windows_error('OpenSCManager') if handle_scm == 0

        handle_scs = OpenService(handle_scm, "fluentdwinsvc", SERVICE_STOP | SERVICE_PAUSE_CONTINUE | SERVICE_USER_DEFINED_CONTROL)
        FFI.raise_windows_error('OpenService') if handle_scs == 0

        control_code = WINSVC_CONTROL_CODE_MAP[command.to_sym]

        unless ControlService(handle_scs, control_code, status)
          FFI.raise_windows_error('ControlService')
        end
      rescue => e
        puts e
        state = status[:dwCurrentState]
        return state == SERVICE_STOPPED || state == SERVICE_STOP_PENDING
      ensure
        CloseServiceHandle(handle_scs)
        CloseServiceHandle(handle_scm)
      end

      return true
    end

    def call_windows_event(command, pid_or_svcname)
      prefix = pid_or_svcname
      event_name = COMMAND_MAP[command.to_sym]
      suffix = event_name.empty? ? "" : "_#{event_name}"

      begin
        event = Win32::Event.open("#{prefix}#{suffix}")
        event.set
        event.close
      rescue Errno::ENOENT
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
      @pid_or_svcname = @argv[1] || "fluentdwinsvc"

      usage("Command isn't specified!") if @command.nil? || @command.empty?
      usage("Unknown command: #{@command}") unless COMMAND_MAP.has_key?(@command.to_sym)

      if Fluent.windows?
        usage("PID or SVCNAME isn't specified!") if @pid_or_svcname.nil? || @pid_or_svcname.empty?
      else
        usage("PID isn't specified!") if @pid_or_svcname.nil? || @pid_or_svcname.empty?
        usage("Invalid PID: #{pid}") unless /^[0-9]+$/.match?(@pid_or_svcname)
      end
    end
  end
end

