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

module Fluent
  module Compat
    module HandleTagNameMixin
      include RecordFilterMixin

      attr_accessor :remove_tag_prefix, :remove_tag_suffix, :add_tag_prefix, :add_tag_suffix
      def configure(conf)
        super

        @remove_tag_prefix = if conf.has_key?('remove_tag_prefix')
                               Regexp.new('^' + Regexp.escape(conf['remove_tag_prefix']))
                             else
                               nil
                             end

        @remove_tag_suffix = if conf.has_key?('remove_tag_suffix')
                               Regexp.new(Regexp.escape(conf['remove_tag_suffix']) + '$')
                             else
                               nil
                             end

        @add_tag_prefix = conf['add_tag_prefix']
        @add_tag_suffix = conf['add_tag_suffix']
      end

      def filter_record(tag, time, record)
        tag.sub!(@remove_tag_prefix, '') if @remove_tag_prefix
        tag.sub!(@remove_tag_suffix, '') if @remove_tag_suffix
        tag.insert(0, @add_tag_prefix) if @add_tag_prefix
        tag << @add_tag_suffix if @add_tag_suffix
        super(tag, time, record)
      end
    end
  end
end
