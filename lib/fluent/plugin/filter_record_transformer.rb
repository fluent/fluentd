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

require 'ostruct'

module Fluent
  class RecordTransformerFilter < Filter
    Fluent::Plugin.register_filter('record_transformer', self)

    def initialize
      require 'socket'
      super
    end

    config_param :remove_keys, :string, :default => nil
    config_param :keep_keys, :string, :default => nil
    config_param :renew_record, :bool, :default => false
    config_param :enable_ruby, :bool, :default => false

    def configure(conf)
      super

      @map = {}
      # <record></record> directive
      conf.elements.select { |element| element.name == 'record' }.each do |element|
        element.each_pair do |k, v|
          element.has_key?(k) # to suppress unread configuration warning
          @map[k] = v
        end
      end

      if @remove_keys
        @remove_keys = @remove_keys.split(',')
      end

      if @keep_keys
        raise Fluent::ConfigError, "`renew_record` must be true to use `keep_keys`" unless @renew_record
        @keep_keys = @keep_keys.split(',')
      end

      @placeholder_expander =
        if @enable_ruby
          # require utilities which would be used in ruby placeholders
          require 'pathname'
          require 'uri'
          require 'cgi'
          RubyPlaceholderExpander.new(log)
        else
          PlaceholderExpander.new(log)
        end

      @hostname = Socket.gethostname
    end

    def filter_stream(tag, es)
      new_es = MultiEventStream.new
      tag_parts = tag.split('.')
      tag_prefix = tag_prefix(tag_parts)
      tag_suffix = tag_suffix(tag_parts)
      placeholders = {
        'tag' => tag,
        'tag_parts' => tag_parts,
        'tag_prefix' => tag_prefix,
        'tag_suffix' => tag_suffix,
        'hostname' => @hostname,
      }
      last_record = nil
      es.each do |time, record|
        last_record = record # for debug log
        new_record = reform(time, record, placeholders)
        new_es.add(time, new_record)
      end
      new_es
    rescue => e
      log.warn "failed to reform records", :error_class => e.class, :error => e.message
      log.warn_backtrace
      log.debug "map:#{@map} record:#{last_record} placeholders:#{placeholders}"
    end

    private

    def reform(time, record, opts)
      @placeholder_expander.prepare_placeholders(time, record, opts)

      new_record = @renew_record ? {} : record.dup
      @keep_keys.each {|k| new_record[k] = record[k]} if @keep_keys and @renew_record
      @map.each_pair {|k, v| new_record[k] = @placeholder_expander.expand(v) }
      @remove_keys.each {|k| new_record.delete(k) } if @remove_keys

      new_record
    end

    def tag_prefix(tag_parts)
      return [] if tag_parts.empty?
      tag_prefix = [tag_parts.first]
      1.upto(tag_parts.size-1).each do |i|
        tag_prefix[i] = "#{tag_prefix[i-1]}.#{tag_parts[i]}"
      end
      tag_prefix
    end

    def tag_suffix(tag_parts)
      return [] if tag_parts.empty?
      rev_tag_parts = tag_parts.reverse
      rev_tag_suffix = [rev_tag_parts.first]
      1.upto(tag_parts.size-1).each do |i|
        rev_tag_suffix[i] = "#{rev_tag_parts[i]}.#{rev_tag_suffix[i-1]}"
      end
      rev_tag_suffix.reverse!
    end

    class PlaceholderExpander
      attr_reader :placeholders, :log

      def initialize(log)
        @log = log
      end

      def prepare_placeholders(time, record, opts)
        placeholders = { '${time}' => Time.at(time).to_s }
        record.each {|key, value| placeholders.store("${#{key}}", value) }

        opts.each do |key, value|
          if value.kind_of?(Array) # tag_parts, etc
            size = value.size
            value.each_with_index { |v, idx|
              placeholders.store("${#{key}[#{idx}]}", v)
              placeholders.store("${#{key}[#{idx-size}]}", v) # support [-1]
            }
          else # string, interger, float, and others?
            placeholders.store("${#{key}}", value)
          end
        end

        @placeholders = placeholders
      end

      def expand(str)
        str.gsub(/(\${[a-zA-Z0-9_]+(\[-?[0-9]+\])?}|__[A-Z_]+__)/) {
          log.warn "unknown placeholder `#{$1}` found" unless @placeholders.include?($1)
          @placeholders[$1]
        }
      end
    end

    class RubyPlaceholderExpander
      attr_reader :placeholders, :log

      def initialize(log)
        @log = log
      end

      # Get placeholders as a struct
      #
      # @param [Time]   time        the time
      # @param [Hash]   record      the record
      # @param [Hash]   opts        others
      def prepare_placeholders(time, record, opts)
        struct = UndefOpenStruct.new(record)
        struct.time = Time.at(time)
        opts.each {|key, value| struct.__send__("#{key}=", value) }
        @placeholders = struct
      end

      # Replace placeholders in a string
      #
      # @param [String] str         the string to be replaced
      def expand(str)
        interpolated = str.gsub(/\$\{([^}]+)\}/, '#{\1}') # ${..} => #{..}
        begin
          eval "\"#{interpolated}\"", @placeholders.instance_eval { binding }
        rescue => e
          log.warn "failed to expand `#{str}`", :error_class => e.class, :error => e.message
          log.warn_backtrace
          nil
        end
      end

      class UndefOpenStruct < OpenStruct
        (Object.instance_methods).each do |m|
          undef_method m unless m.to_s =~ /^__|respond_to_missing\?|object_id|public_methods|instance_eval|method_missing|define_singleton_method|respond_to\?|new_ostruct_member/
        end
      end
    end
  end
end
