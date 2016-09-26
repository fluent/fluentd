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

require 'fluent/counter/error'

module Fluent
  module Counter
    class Validator
      VALID_NAME = /\A[a-z][a-zA-Z0-9\-_]*\Z/
      VALID_SCOPE_NAME = /\A[a-z][\ta-zA-Z0-9\-_]*\Z/
      VALID_METHODS = %w(establish init delete inc get reset)

      def self.request(data)
        errors = []
        raise "Received data is not Hash: #{data}" unless data.is_a?(Hash)

        unless data['id']
          errors << Fluent::Counter::InvalidRequest.new('Request should include `id`')
        end

        if !data['method']
          errors << Fluent::Counter::InvalidRequest.new('Request should include `method`')
        elsif !(VALID_NAME =~ data['method'])
          errors << Fluent::Counter::InvalidRequest.new('`method` is the invalid format')
        elsif !VALID_METHODS.include?(data['method'])
          errors << Fluent::Counter::MethodNotFound.new("Unknown method name passed: #{data['method']}")
        end

        errors.map(&:to_hash)
      end

      def initialize(*types)
        @types = types.map(&:to_s)
        @empty = @types.delete('empty')
      end

      def call(data)
        success = []
        errors = []

        if @empty && data.empty?
          errors << Fluent::Counter::InvalidParams.new('One or more `params` are required')
        else
          data.each do |d|
            begin
              @types.each { |type| dispatch(type, d) }
              success << d
            rescue => e
              errors << e
            end
          end
        end

        [success, errors]
      end

      private

      def dispatch(type, data)
        send("validate_#{type}!", data)
      rescue NoMethodError => e
        raise Fluent::Counter::InternalServerError.new(e)
      end
    end

    class ArrayValidator < Validator
      def validate_key!(name)
        unless name.is_a?(String)
          raise Fluent::Counter::InvalidParams.new('The type of `key` should be String')
        end

        unless VALID_NAME =~ name
          raise Fluent::Counter::InvalidParams.new('`key` is the invalid format')
        end
      end

      def validate_scope!(name)
        unless name.is_a?(String)
          raise Fluent::Counter::InvalidParams.new('The type of `scope` should be String')
        end

        unless VALID_SCOPE_NAME =~ name
          raise Fluent::Counter::InvalidParams.new('`scope` is the invalid format')
        end
      end
    end

    class HashValidator < Validator
      def validate_name!(hash)
        name = hash['name']
        unless name
          raise Fluent::Counter::InvalidParams.new('`name` is required')
        end

        unless name.is_a?(String)
          raise Fluent::Counter::InvalidParams.new('The type of `name` should be String')
        end

        unless VALID_NAME =~ name
          raise Fluent::Counter::InvalidParams.new("`name` is the invalid format")
        end
      end

      def validate_value!(hash)
        value = hash['value']
        unless value
          raise Fluent::Counter::InvalidParams.new('`value` is required')
        end

        unless value.is_a?(Numeric)
          raise Fluent::Counter::InvalidParams.new("The type of `value` type should be Numeric")
        end
      end

      def validate_reset_interval!(hash)
        interval = hash['reset_interval']

        unless interval
          raise Fluent::Counter::InvalidParams.new('`reset_interval` is required')
        end

        unless interval.is_a?(Numeric)
          raise Fluent::Counter::InvalidParams.new('The type of `reset_interval` should be Numeric')
        end

        if interval < 0
          raise Fluent::Counter::InvalidParams.new('`reset_interval` should be a positive number')
        end
      end
    end
  end
end
