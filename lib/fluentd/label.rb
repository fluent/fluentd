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

  require 'fluentd/agent'
  require 'fluentd/has_nested_match'

  #
  # Label is an Agent with nested <match> elements.
  #
  # <source> or <match> with HasNestedMatch can emit records to
  # the <match> elements of this Label through Label#collector.
  # Label#collector is an EventRouter initialized at
  # HasNestedMatch#initialize.
  #
  # Next step: 'fluentd/has_nested_match.rb'
  #
  class Label < Agent
    include HasNestedMatch
  end
end
