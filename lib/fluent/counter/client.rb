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
require 'fluent/counter/error'
require 'timeout'

module Fluent
  module Counter
    class Client
      DEFAULT_PORT = 24321
      DEFAULT_ADDR = '127.0.0.1'
      DEFAULT_TIMEOUT = 5
      ID_LIMIT_COUNT = 1 << 31

      def initialize(loop = nil, opt = {})
        @loop = loop || Coolio::Loop.new
        @port = opt[:port] || DEFAULT_PORT
        @host = opt[:host] || DEFAULT_ADDR
        @log = opt[:log] || $log
        @timeout = opt[:timeout] || DEFAULT_TIMEOUT
        @conn = Connection.connect(@host, @port, method(:on_message))
        @responses = {}
        @id = 0
        @id_mutex = Mutex.new
        @loop_mutex = Mutex.new
      end

      def start
        @loop.attach(@conn)
        @log.debug("starting counter client: #{@host}:#{@port}")
        self
      rescue => e
        if @log
          @log.error e
        else
          STDERR.puts e
        end
      end

      def stop
        @conn.close
        @log.debug("calling stop in counter client: #{@host}:#{@port}")
      end

      def establish(scope)
        scope = Timeout.timeout(@timeout) {
          response = send_request('establish', nil, [scope])
          Fluent::Counter.raise_error(response.errors.first) if response.errors?
          data = response.data
          data.first
        }
        @scope = scope
      rescue Timeout::Error
        raise "Can't establish the connection to counter server due to timeout"
      end

      # === Example
      # `init` receives various arguments.
      #
      # 1. init(name: 'name')
      # 2. init({ name: 'name',reset_interval: 20 }, options: {})
      # 3. init([{ name: 'name1',reset_interval: 20 }, { name: 'name2',reset_interval: 20 }])
      # 4. init([{ name: 'name1',reset_interval: 20 }, { name: 'name2',reset_interval: 20 }], options: {})
      # 5. init([{ name: 'name1',reset_interval: 20 }, { name: 'name2',reset_interval: 20 }]) { |res| ... }
      def init(params, options: {})
        exist_scope!
        params = [params] unless params.is_a?(Array)
        res = send_request('init', @scope, params, options)

        # if `async` is false or missing, block at this method and return a Future::Result object.
        if block_given?
          Thread.start do
            yield res.get
          end
        else
          res
        end
      end

      def delete(*params, options: {})
        exist_scope!
        res = send_request('delete', @scope, params, options)

        if block_given?
          Thread.start do
            yield res.get
          end
        else
          res
        end
      end

      # === Example
      # `inc` receives various arguments.
      #
      # 1. inc(name: 'name')
      # 2. inc({ name: 'name',value: 20 }, options: {})
      # 3. inc([{ name: 'name1',value: 20 }, { name: 'name2',value: 20 }])
      # 4. inc([{ name: 'name1',value: 20 }, { name: 'name2',value: 20 }], options: {})
      def inc(params, options: {})
        exist_scope!
        params = [params] unless params.is_a?(Array)
        res = send_request('inc', @scope, params, options)

        if block_given?
          Thread.start do
            yield res.get
          end
        else
          res
        end
      end

      def get(*params, options: {})
        exist_scope!
        res = send_request('get', @scope, params, options)

        if block_given?
          Thread.start do
            yield res.get
          end
        else
          res
        end
      end

      def reset(*params, options: {})
        exist_scope!
        res = send_request('reset', @scope, params, options)

        if block_given?
          Thread.start do
            yield res.get
          end
        else
          res
        end
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
        @log.debug(request)
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
      class Result
        attr_reader :data, :errors

        def initialize(result)
          @errors = result['errors']
          @data = result['data']
        end

        def success?
          @errors.nil? || @errors.empty?
        end

        def error?
          !success?
        end
      end

      def initialize(loop, mutex)
        @set = false
        @result = nil
        @mutex = mutex
        @loop = loop
      end

      def set(v)
        @result = Result.new(v)
        @set = true
      end

      def errors
        get.errors
      end

      def errors?
        es = errors
        es && !es.empty?
      end

      def data
        get.data
      end

      def get
        # Block until `set` method is called and @result is set
        join if @result.nil?
        @result
      end

      def wait
        res = get
        if res.error?
          Fluent::Counter.raise_error(res.errors.first)
        end
        res
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
