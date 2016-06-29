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

require 'fluent/compat/record_filter_mixin'
require 'fluent/compat/handle_tag_name_mixin'
require 'fluent/compat/set_time_key_mixin'
require 'fluent/compat/set_tag_key_mixin'
require 'fluent/compat/type_converter'

require 'fluent/time' # Fluent::TimeFormatter

module Fluent
  RecordFilterMixin = Fluent::Compat::RecordFilterMixin
  HandleTagNameMixin = Fluent::Compat::HandleTagNameMixin
  SetTimeKeyMixin = Fluent::Compat::SetTimeKeyMixin
  SetTagKeyMixin = Fluent::Compat::SetTagKeyMixin
  TypeConverter = Fluent::Compat::TypeConverter
end
