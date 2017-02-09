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

require 'fluent/compat/output'
require 'fluent/output_chain'

module Fluent
  Output               = Fluent::Compat::Output
  BufferedOutput       = Fluent::Compat::BufferedOutput
  ObjectBufferedOutput = Fluent::Compat::ObjectBufferedOutput
  TimeSlicedOutput     = Fluent::Compat::TimeSlicedOutput
  MultiOutput          = Fluent::Compat::MultiOutput

  # Some input plugins refer BufferQueueLimitError for throttling
  BufferQueueLimitError = Fluent::Compat::BufferQueueLimitError
end
