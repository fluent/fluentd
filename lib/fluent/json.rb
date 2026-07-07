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
require 'json'

module Fluent
  module JSONResumableParserEmptyPredicate
    unless JSON::ResumableParser.method_defined?(:empty?)
      # This is for json <= 2.20.0
      refine JSON::ResumableParser do
        def empty?
          rest.empty? && partial_value.nil?
        end
      end
    end
  end
end
