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
    @patterns = pattern_str.split(/\s+/).map {|str|
      MatchPattern.create(str)
    }
    @output = output
  end

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
    if @patterns.any? {|pattern| pattern.match(tag) }
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

class RegexMatchPattern < MatchPattern
  def initialize(regex)
    @regex = regex
  end

  def match(str)
    @regex.match(str) != nil
  end
end

class GlobMatchPattern < MatchPattern
  def initialize(pat)
    regex = ''
    escape = false
    # FIXME
    pat.scan(/./) {|c|
      if escape
        regex << c
        escape = false
      elsif c == '/'
        escape = true
      elsif c =~ /[a-zA-Z0-9_]/
        regex << c
      elsif c == '*'
        regex << '[^\.]*'
      elsif c == '?'
        regex << '.'
      else
        regex << "\\#{c}"
      end
    }
    @regex = Regexp.new(regex)
  end

  def match(str)
    @regex.match(str) != nil
  end
end


end

