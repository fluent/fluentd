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

require 'fluent/config/error'
require 'fluent/config/types'
require 'fluent/compat/record_filter_mixin'

module Fluent
  module Compat
    module SetTagKeyMixin
      include RecordFilterMixin

      attr_accessor :include_tag_key, :tag_key

      def configure(conf)
        @include_tag_key = false

        super

        if s = conf['include_tag_key']
          include_tag_key = Fluent::Config.bool_value(s)
          raise Fluent::ConfigError, "Invalid boolean expression '#{s}' for include_tag_key parameter" if include_tag_key.nil?

          @include_tag_key = include_tag_key
        end

        @tag_key = conf['tag_key'] || 'tag' if @include_tag_key
      end

      def filter_record(tag, time, record)
        super

        record[@tag_key] = tag if @include_tag_key
      end
    end
  end
end
