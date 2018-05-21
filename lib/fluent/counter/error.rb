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
  module Counter
    class BaseError < StandardError
      def to_hash
        { 'code' => code, 'message' => message }
      end

      def code
        raise NotImplementedError
      end
    end

    class InvalidParams < BaseError
      def code
        'invalid_params'
      end
    end

    class UnknownKey < BaseError
      def code
        'unknown_key'
      end
    end

    class ParseError < BaseError
      def code
        'parse_error'
      end
    end

    class InvalidRequest < BaseError
      def code
        'invalid_request'
      end
    end

    class MethodNotFound < BaseError
      def code
        'method_not_found'
      end
    end

    class InternalServerError < BaseError
      def code
        'internal_server_error'
      end
    end

    def raise_error(response)
      msg = response['message']
      case response['code']
      when 'invalid_params'
        raise InvalidParams.new(msg)
      when 'unknown_key'
        raise UnknownKey.new(msg)
      when 'parse_error'
        raise ParseError.new(msg)
      when 'invalid_request'
        raise InvalidRequest.new(msg)
      when 'method_not_found'
        raise MethodNotFound.new(msg)
      when 'internal_server_error'
        raise InternalServerError.new(msg)
      else
        raise "Unknown code: #{response['code']}"
      end
    end
    module_function :raise_error
  end
end
