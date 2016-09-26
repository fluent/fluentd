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
  end
end
