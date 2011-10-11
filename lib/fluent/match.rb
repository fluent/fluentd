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


class Match
  def initialize(pattern_str, output)
    patterns = pattern_str.split(/\s+/).map {|str|
      MatchPattern.create(str)
    }
    if patterns.length == 1
      @pattern = patterns[0]
    else
      @pattern = OrMatchPattern.new(patterns)
    end
    @output = output
  end

  attr_reader :output

  def emit(tag, es)
    chain = NullOutputChain.instance
    @output.emit(tag, es, chain)
  end

  def start
    @output.start
  end

  def shutdown
    @output.shutdown
  end

  def match(tag)
    if @pattern.match(tag)
      return true
    end
    return false
  end
end


class MatchPattern
  def self.create(str)
    GlobMatchPattern.new(str)
  end

  #def match(str)
  #end
end

## TODO
#class RegexMatchPattern < MatchPattern
#  def initialize(regex)
#    @regex = regex
#  end
#
#  def match(str)
#    @regex.match(str) != nil
#  end
#end

class GlobMatchPattern < MatchPattern
  def initialize(pat)
    stack = []
    regex = ['']
    escape = false
    dot = false

    i = 0
    while i < pat.length
      c = pat[i,1]

      if escape
        regex.last << Regexp.escape(c)
        escape = false
        i += 1
        next

      elsif pat[i,2] == "**"
        # recursive any
        if dot
          regex.last << "(?![^\\.])"
          dot = false
        end
        if pat[i+2,1] == "."
          regex.last << "(?:.*\\.|\\A)"
          i += 3
        else
          regex.last << ".*"
          i += 2
        end
        next

      elsif dot
        regex.last << "\\."
        dot = false
      end

      if c == "\\"
        escape = true

      elsif c == "."
        dot = true

      elsif c == "*"
        # any
        regex.last << "[^\\.]*"

      # TODO
      #elsif c == "["
      #  # character class
      #  chars = ''
      #  while i < pat.length
      #    c = pat[i,1]
      #    if c == "]"
      #      break
      #    else
      #      chars << c
      #    end
      #    i += 1
      #  end
      #  regex.last << '['+Regexp.escape(chars).gsub("\\-",'-')+']'

      elsif c == "{"
        # or
        stack.push []
        regex.push ''

      elsif c == "}" && !stack.empty?
        stack.last << regex.pop
        regex.last << Regexp.union(*stack.pop.map {|r| Regexp.new(r) }).to_s

      elsif c == "," && !stack.empty?
        stack.last << regex.pop
        regex.push ''

      elsif c =~ /[a-zA-Z0-9_]/
        regex.last << c

      else
        regex.last << "\\#{c}"
      end

      i += 1
    end

    until stack.empty?
      stack.last << regex.pop
      regex.last << Regexp.union(*stack.pop).to_s
    end

    @regex = Regexp.new("\\A"+regex.last+"\\Z")
  end

  def match(str)
    @regex.match(str) != nil
  end
end


class OrMatchPattern < MatchPattern
  def initialize(patterns)
    @patterns = patterns
  end

  def match(str)
    @patterns.any? {|pattern| pattern.match(str) }
  end
end


end

