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

module Fluent
  module Compat
    module StringUtil
      def match_regexp(regexp, string)
        begin
          return regexp.match(string)
        rescue ArgumentError => e
          raise e unless e.message.index("invalid byte sequence in".freeze).zero?
          $log.info "invalid byte sequence is replaced in `#{string}`"
          string = string.scrub('?')
          retry
        end
        return true
      end
      module_function :match_regexp
    end
  end
end
