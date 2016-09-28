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

require 'cool.io'
require 'fluent/counter/base_socket'

module Fluent
  module Counter
    class Client
      DEFAULT_PORT = 4321
      DEFAULT_HOST = '127.0.0.1'
      ID_LIMIT_COUNT = 1 << 31

      def initialize(loop = Coolio::Loop.new, opt = {})
        @loop = loop
        @port = opt[:port] || DEFAULT_PORT
        @host = opt[:host] || DEFAULT_HOST
        @log = opt[:log] || $log
        @conn = Connection.connect(@host, @port, method(:on_message))
        @responses = {}
        @id = 0
        @id_mutex = Mutex.new
        @loop_mutex = Mutex.new
      end

      def start
        @loop.attach(@conn)
        self
      rescue => e
        @log.error e
      end

      def stop
        @conn.close
      end

      def establish(scope)
        response = send_request('establish', nil, [scope])

        raise response.errors.first if response.errors?
        data = response.data
        @scope = data.first
      end

      # === Example
      # `init` receives various arguments.
      #
      # 1. init(name: 'name')
      # 2. init({ name: 'name',reset_interval: 20 }, options: {})
      # 3. init([{ name: 'name1',reset_interval: 20 }, { name: 'name2',reset_interval: 20 }])
      # 4. init([{ name: 'name1',reset_interval: 20 }, { name: 'name2',reset_interval: 20 }], options: {})
      def init(params, options: {})
        exist_scope!
        params = [params] unless params.is_a?(Array)
        res = send_request('init', @scope, params, options)

        # if `async` is true, return a Future object (non blocking).
        # if `async` is false or missing, block at this method and return a Hash object.
        options[:async] ? res : res.get
      end

      def delete(*params, options: {})
        exist_scope!
        res = send_request('delete', @scope, params, options)
        options[:async] ? res : res.get
      end

      # === Example
      # `inc` receives various arguments.
      #
      # 1. init(name: 'name')
      # 2. init({ name: 'name',value: 20 }, options: {})
      # 3. init([{ name: 'name1',value: 20 }, { name: 'name2',value: 20 }])
      # 4. init([{ name: 'name1',value: 20 }, { name: 'name2',value: 20 }], options: {})
      def inc(params, options: {})
        exist_scope!
        params = [params] unless params.is_a?(Array)
        res = send_request('inc', @scope, params, options)
        options[:async] ? res : res.get
      end

      def get(*params, options: {})
        exist_scope!
        res = send_request('get', @scope, params, options)
        options[:async] ? res : res.get
      end

      def reset(*params, options: {})
        exist_scope!
        res = send_request('reset', @scope, params, options)
        options[:async] ? res : res.get
      end

      private

      def exist_scope!
        raise 'Call `establish` method to get a `scope` before calling this method' unless @scope
      end

      def on_message(data)
        if response = @responses.delete(data['id'])
          response.set(data)
        else
          @log.warn("Receiving missing id data: #{data}")
        end
      end

      def send_request(method, scope, params, opt = {})
        id = generate_id
        res = Future.new(@loop, @loop_mutex)
        @responses[id] = res # set a response value to this future object at `on_message`
        request = build_request(method, id, scope, params, opt)
        @log.debug(request) if @log
        @conn.send_data request
        res
      end

      def build_request(method, id, scope = nil, params = nil, options = nil)
        r = { id: id, method: method }
        r[:scope] = scope if scope
        r[:params] = params if params
        r[:options] = options if options
        r
      end

      def generate_id
        id = 0
        @id_mutex.synchronize do
          id = @id
          @id += 1
          @id = 0 if ID_LIMIT_COUNT < @id
        end
        id
      end
    end

    class Connection < Fluent::Counter::BaseSocket
      def initialize(io, on_message)
        super(io)
        @connection = false
        @buffer = ''
        @on_message = on_message
      end

      def send_data(data)
        if @connection
          packed_write data
        else
          @buffer += pack(data)
        end
      end

      def on_connect
        @connection = true
        write @buffer
        @buffer = ''
      end

      def on_close
        @connection = false
      end

      def on_message(data)
        @on_message.call(data)
      end
    end

    class Future
      def initialize(loop, mutex)
        @set = false
        @result = nil
        @mutex = mutex
        @loop = loop
      end

      def set(v)
        @result = v
        @set = true
      end

      def errors
        get['errors']
      end

      def errors?
        es = errors
        es && !es.empty?
      end

      def data
        get['data']
      end

      def get
        # Block until `set` method is called and @result is set a value
        join if @result.nil?
        @result
      end

      private

      def join
        until @set
          @mutex.synchronize do
            @loop.run_once(0.0001) # retun a lock as soon as possible
          end
        end
      end
    end
  end
end
