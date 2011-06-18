#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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
module Fluent

class Log
	LEVEL_TRACE = 0
	LEVEL_DEBUG = 1
	LEVEL_INFO  = 2
	LEVEL_WARN  = 3
	LEVEL_ERROR = 4
	LEVEL_FATAL = 5

	def initialize(level = LEVEL_TRACE, out = $stderr)
		@level = level
		@out = out
		@debug_mode = false
	end

	attr_accessor :out
	attr_accessor :level

	def enable_debug(b=true)
		@debug_mode = b
	end

	def on_trace(&block)
		return if @level > LEVEL_TRACE
		block.call if block
	end

	def trace(*args, &block)
		return if @level > LEVEL_TRACE
		args << block.call if block
		msg = args.join
		puts "#{caller_line(1,true)}#{msg}"
	end
	alias TRACE trace

	def on_debug(&block)
		return if @level > LEVEL_DEBUG
		block.call if block
	end

	def debug(*args, &block)
		return if @level > LEVEL_DEBUG
		args << block.call if block
		msg = args.join
		puts "#{caller_line(1,true)}#{msg}"
	end
	alias DEBUG debug

	def debug_backtrace(backtrace=$!.backtrace)
		return if @level > LEVEL_DEBUG
		backtrace.each {|msg|
			puts "  #{caller_line(4,true)}#{msg}"
		}
		nil
	end

	def on_info(&block)
		return if @level > LEVEL_INFO
		block.call if block
	end

	def info(*args, &block)
		return if @level > LEVEL_INFO
		args << block.call if block
		msg = args.join
		puts "#{caller_line(1,true)}#{msg}"
	end
	alias INFO info

	def info_backtrace(backtrace=$!.backtrace)
		return if @level > LEVEL_INFO
		backtrace.each {|msg|
			puts "  #{caller_line(4,true)}#{msg}"
		}
		nil
	end

	def on_warn(&block)
		return if @level > LEVEL_WARN
		block.call if block
	end

	def warn(*args, &block)
		return if @level > LEVEL_WARN
		args << block.call if block
		msg = args.join
		puts "#{caller_line(1)}#{msg}"
	end
	alias WARN warn

	def warn_backtrace(backtrace=$!.backtrace)
		return if @level > LEVEL_WARN
		backtrace.each {|msg|
			puts "  #{caller_line(4)}#{msg}"
		}
		nil
	end

	def on_error(&block)
		return if @level > LEVEL_ERROR
		block.call if block
	end

	def error(*args, &block)
		return if @level > LEVEL_ERROR
		args << block.call if block
		msg = args.join
		puts "#{caller_line(1)}#{msg}"
	end
	alias ERROR error

	def error_backtrace(backtrace=$!.backtrace)
		return if @level > LEVEL_ERROR
		backtrace.each {|msg|
			puts "  #{caller_line(4)}#{msg}"
		}
		nil
	end

	def on_fatal(&block)
		return if @level > LEVEL_FATAL
		block.call if block
	end

	def fatal(*args, &block)
		return if @level > LEVEL_FATAL
		args << block.call if block
		msg = args.join
		puts "#{caller_line(1)}#{msg}"
	end
	alias FATAL fatal

	def fatal_backtrace(backtrace=$!.backtrace)
		return if @level > LEVEL_FATAL
		backtrace.each {|msg|
			puts "  #{caller_line(4)}#{msg}"
		}
		nil
	end

	def puts(msg)
		@out.puts(msg)
		@out.flush
		msg
	rescue
		# FIXME
		nil
	end

	private
	def caller_line(level, debug = false)
		line = caller(level+1)[0]
		if @debug_mode
			if match = /^(.+?):(\d+)(?::in `(.*)')?/.match(line)
				if debug
					return "#{match[1]}:#{match[2]}:#{match[3]}: "
				else
					return "#{match[1]}:#{match[2]}: "
				end
			end
		end
		""
	end
end

end

