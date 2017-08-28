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
require 'fluent/config/literal_parser'

module Fluent
  module Config
    class Element < Hash
      def initialize(name, arg, attrs, elements, unused = nil)
        @name = name
        @arg = arg
        @elements = elements
        super()
        attrs.each { |k, v|
          self[k] = v
        }
        @unused = unused || attrs.keys
        @v1_config = false
        @corresponding_proxies = [] # some plugins use flat parameters, e.g. in_http doesn't provide <format> section for parser.
        @unused_in = false # if this element is not used in plugins, correspoing plugin name and parent element name is set, e.g. [source, plugin class].

        # it's global logger, not plugin logger: deprecated message should be global warning, not plugin level.
        @logger = defined?($log) ? $log : nil

        @target_worker_id = nil
      end

      attr_accessor :name, :arg, :unused, :v1_config, :corresponding_proxies, :unused_in
      attr_writer :elements
      attr_reader :target_worker_id

      RESERVED_PARAMETERS_COMPAT = {
        '@type' => 'type',
        '@id' => 'id',
        '@log_level' => 'log_level',
        '@label' => nil,
      }
      RESERVED_PARAMETERS = RESERVED_PARAMETERS_COMPAT.keys

      def elements(*names, name: nil, arg: nil)
        raise ArgumentError, "name and names are exclusive" if name && !names.empty?
        raise ArgumentError, "arg is available only with name" if arg && !name

        if name
          @elements.select{|e| e.name == name && (!arg || e.arg == arg) }
        elsif !names.empty?
          @elements.select{|e| names.include?(e.name) }
        else
          @elements
        end
      end

      def add_element(name, arg = '')
        e = Element.new(name, arg, {}, [])
        e.v1_config = @v1_config
        @elements << e
        e
      end

      def inspect
        attrs = super
        "name:#{@name}, arg:#{@arg}, " + attrs + ", " + @elements.inspect
      end

      # Used by PP and Pry
      def pretty_print(q)
        q.text(inspect)
      end

      # This method assumes _o_ is an Element object. Should return false for nil or other object
      def ==(o)
        self.name == o.name && self.arg == o.arg &&
          self.keys.size == o.keys.size &&
          self.keys.reduce(true){|r, k| r && self[k] == o[k] } &&
          self.elements.size == o.elements.size &&
          [self.elements, o.elements].transpose.reduce(true){|r, e| r && e[0] == e[1] }
      end

      def +(o)
        e = Element.new(@name.dup, @arg.dup, o.merge(self), @elements + o.elements, (@unused + o.unused).uniq)
        e.v1_config = @v1_config
        e
      end

      # no code in fluentd uses this method
      def each_element(*names, &block)
        if names.empty?
          @elements.each(&block)
        else
          @elements.each { |e|
            if names.include?(e.name)
              block.yield(e)
            end
          }
        end
      end

      def has_key?(key)
        @unused_in = false # some sections, e.g. <store> in copy, is not defined by config_section so clear unused flag for better warning message in check_not_fetched.
        @unused.delete(key)
        super
      end

      def [](key)
        @unused_in = false # ditto
        @unused.delete(key)

        if RESERVED_PARAMETERS.include?(key) && !has_key?(key) && has_key?(RESERVED_PARAMETERS_COMPAT[key])
          @logger.warn "'#{RESERVED_PARAMETERS_COMPAT[key]}' is deprecated parameter name. use '#{key}' instead." if @logger
          return self[RESERVED_PARAMETERS_COMPAT[key]]
        end

        super
      end

      def check_not_fetched(&block)
        each_key { |key|
          if @unused.include?(key)
            block.call(key, self)
          end
        }
        @elements.each { |e|
          e.check_not_fetched(&block)
        }
      end

      def to_s(nest = 0)
        indent = "  " * nest
        nindent = "  " * (nest + 1)
        out = ""
        if @arg.empty?
          out << "#{indent}<#{@name}>\n"
        else
          out << "#{indent}<#{@name} #{@arg}>\n"
        end
        each_pair { |k, v|
          out << dump_value(k, v, nindent)
        }
        @elements.each { |e|
          out << e.to_s(nest + 1)
        }
        out << "#{indent}</#{@name}>\n"
        out
      end

      def to_masked_element
        new_elems = @elements.map { |e| e.to_masked_element }
        new_elem = Element.new(@name, @arg, {}, new_elems, @unused)
        new_elem.v1_config = @v1_config
        new_elem.corresponding_proxies = @corresponding_proxies
        each_pair { |k, v|
          new_elem[k] = secret_param?(k) ? 'xxxxxx' : v
        }
        new_elem
      end

      def secret_param?(key)
        return false if @corresponding_proxies.empty?

        param_key = key.to_sym
        @corresponding_proxies.each { |proxy|
          _block, opts = proxy.params[param_key]
          if opts && opts.has_key?(:secret)
            return opts[:secret]
          end
        }

        false
      end

      def param_type(key)
        return nil if @corresponding_proxies.empty?

        param_key = key.to_sym
        proxy = @corresponding_proxies.detect do |_proxy|
          _proxy.params.has_key?(param_key)
        end
        return nil unless proxy
        _block, opts = proxy.params[param_key]
        opts[:type]
      end

      def dump_value(k, v, nindent)
        if secret_param?(k)
          "#{nindent}#{k} xxxxxx\n"
        else
          if @v1_config
            case param_type(k)
            when :string
              "#{nindent}#{k} \"#{self.class.unescape_parameter(v)}\"\n"
            when :enum, :integer, :float, :size, :bool, :time
              "#{nindent}#{k} #{v}\n"
            when :hash, :array
              "#{nindent}#{k} #{v}\n"
            else
              # Unknown type
              "#{nindent}#{k} #{v}\n"
            end
          else
            "#{nindent}#{k} #{v}\n"
          end
        end
      end

      def self.unescape_parameter(v)
        result = ''
        v.each_char { |c| result << LiteralParser.unescape_char(c) }
        result
      end

      def set_target_worker_id(worker_id)
        @target_worker_id = worker_id
        @elements.each { |e|
          e.set_target_worker_id(worker_id)
        }
      end

      def for_every_workers?
        @target_worker_id == nil
      end

      def for_this_worker?
        @target_worker_id == Fluent::Engine.worker_id
      end

      def for_another_worker?
        @target_worker_id != nil && @target_worker_id != Fluent::Engine.worker_id
      end
    end
  end
end
