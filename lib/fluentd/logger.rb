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
module Fluentd

  require 'serverengine'

  class Logger < ServerEngine::DaemonLogger
    def initialize(logdev, config={})
      super

      @enable_filename = true
      @time_format = '%Y-%m-%d %H:%M:%S %z '

      self.enable_color = logdev.respond_to?(:tty?) && logdev.tty?
    end

    attr_accessor :time_format

    attr_accessor :enable_filename

    def enable_color=(bool)
      if bool
        @level_colors = LEVEL_COLORS
        @reset_color = TTY_NORMAL
      else
        @level_colors = []
        @reset_color = nil
      end
      self
    end

    def enable_color
      @reset_color != nil
    end

    TTY_RESET   = "\033]R"
    TTY_CRE     = "\033[K"
    TTY_CLEAR   = "\033c"
    TTY_NORMAL  = "\033[0;39m"
    TTY_RED     = "\033[1;31m"
    TTY_GREEN   = "\033[1;32m"
    TTY_YELLOW  = "\033[1;33m"
    TTY_BLUE    = "\033[1;34m"
    TTY_MAGENTA = "\033[1;35m"
    TTY_CYAN    = "\033[1;36m"
    TTY_WHITE   = "\033[1;37m"

    LEVEL_NAMES  = %w[trace     debug      info       warn        error        fatal]
    LEVEL_COLORS =   [TTY_BLUE, TTY_WHITE, TTY_GREEN, TTY_YELLOW, TTY_MAGENTA, TTY_RED]

    #
    # 1) override logging methods to support structured events
    # 2) add on_{level}(&block) methods
    # 3) add {level}_backtrace(backtrace=$!.backtrace) methods
    #
    %w[trace debug info warn error fatal].each_with_index do |name,level|
      eval <<-CODE
      def on_#{name}(&block)
        return if @level > #{level-1}
        block.call if block
      end

      def #{name}(*args, &block)
        return if @level > #{level-1}

        message = ''

        record = {}
        args.each {|a|
          if a.is_a?(Hash)
            a.each_pair {|k,v|
              record[k.to_s] = v
            }
          else
            message << a.to_s
          end
        }

        message << block.call if block

        add_event(#{level}, Time.now, message, record, caller(1))
      end

      def #{name}_backtrace(backtrace=$!.backtrace)
        return if @level > #{level-1}
        add_backtrace(#{level}, Time.now, backtrace, caller(1))
      end
      CODE
    end

    private

    def add_event(level, time, message, record, caller_stack)
      self << format_event(level, time, message, record, caller_stack)
      nil
    end

    def add_backtrace(level, time, backtrace, caller_stack)
      backtrace.each {|message|
        self << format_event(level, time, "  #{message}", nil, caller_stack)
      }

      nil
    end

    def format_event(level, time, message, record, caller_stack)
      time_str = time.strftime(@time_format)

      unless record.nil? || record.empty?
        message = "#{message}:"
        record.each_pair {|k,v|
          message << " #{k}=#{v}"
        }
      end

      if @enable_filename && m = /^(.+?):(\d+)(?::in `(.*)')?/.match(caller_stack.first || '')
        dir_fname = m[1].split('/')[-2,2]
        file = dir_fname ? dir_fname.join('/') : m[1]
        line = m[2]
        method = m[3]
        return "#{@level_colors[level]}#{time_str}[#{LEVEL_NAMES[level]}]: #{file}:#{line}:#{method}: #{message}#{@reset_color}\n"
      else
        return "#{@level_colors[level]}#{time_str}[#{LEVEL_NAMES[level]}]: #{message}#{@reset_color}\n"
      end
    end
  end

end
