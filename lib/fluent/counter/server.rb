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
require 'fluent/counter/validator'
require 'fluent/counter/store'
require 'fluent/counter/mutex_hash'

module Fluent
  module Counter
    class Server
      DEFAULT_HOST = '127.0.0.1'
      DEFAULT_PORT = 4321

      def initialize(opt = {})
        @opt = opt
        @host = @opt[:host] || DEFAULT_HOST
        @port = @opt[:port] || DEFAULT_PORT
        @loop = @opt[:loop] || Coolio::Loop.new

        @counter = Fluent::Counter::Counter.new(opt)
        @server = Coolio::TCPServer.new(@host, @port, Handler, @counter.method(:on_message))
        @thread = nil
        @run = false
      end

      def start
        @server.attach(@loop)
        @thread = Thread.new do
          @loop.run(0.5)
          @run = true
        end
        self
      end

      def stop
        # This `sleep` for a test to wait for a `@loop` to begin to run
        sleep 0.1 unless @run
        @server.close
        @loop.stop
        @thread.join if @thread
      end
    end

    class Counter
      def initialize(opt = {})
        @opt = opt
        @store = Fluent::Counter::Store.new
        @mutex_hash = MutexHash.new(@store)
      end

      def on_message(data)
        errors = Validator.request(data)
        unless errors.empty?
          return { 'id' => data['id'], 'data' => [], 'errors' => errors }
        end

        result = safe_run do
          send(data['method'], data['params'], data['scope'], data['options'])
        end
        result.merge('id' => data['id'])
      end

      private

      def establish(params, _scope, _options)
        validator = Fluent::Counter::ArrayValidator.new(:empty, :scope)
        valid_params, errors = validator.call(params)
        res = Response.new(errors)

        valid_params.take(1).each do |param|
          # TODO
          res.push_data "somthing_name\t#{param}"
        end

        res.to_hash
      end

      def init(params, scope, options)
        validator = Fluent::Counter::HashValidator.new(:empty, :name, :reset_interval)
        valid_params, errors = validator.call(params)
        res = Response.new(errors)
        vp = valid_params.map { |e| e['name'] }

        do_init = lambda do |store, key|
          begin
            param = valid_params.find { |par| par['name'] == key }
            v = store.init(key, scope, param, ignore: options['ignore'])
            res.push_data v
          rescue => e
            res.push_error e
          end
        end

        if options['random']
          @mutex_hash.synchronize_keys(*vp, &do_init)
        else
          @mutex_hash.synchronize(*vp, &do_init)
        end

        res.to_hash
      end

      def delete(params, scope, options)
        validator = Fluent::Counter::ArrayValidator.new(:empty, :key)
        valid_params, errors = validator.call(params)
        res = Response.new(errors)

        do_delete = lambda do |store, key|
          begin
            v = store.delete(key, scope)
            res.push_data v
          rescue => e
            res.push_error e
          end
        end

        if options['random']
          @mutex_hash.synchronize_keys(*valid_params, &do_delete)
        else
          @mutex_hash.synchronize(*valid_params, &do_delete)
        end

        res.to_hash
      end

      def inc(params, scope, options)
        validate_param = [:empty, :name, :value]
        validate_param << :reset_interval if options['force']
        validator = Fluent::Counter::HashValidator.new(*validate_param)

        valid_params, errors = validator.call(params)
        res = Response.new(errors)
        vp = valid_params.map { |par| par['name'] }

        do_inc = lambda do |store, key|
          begin
            param = valid_params.find { |par| par['name'] == key }
            v = store.inc(key, scope, param, force: options['force'])
            res.push_data v
          rescue => e
            res.push_error e
          end
        end

        if options['random']
          @mutex_hash.synchronize_keys(*vp, &do_inc)
        else
          @mutex_hash.synchronize(*vp, &do_inc)
        end

        res.to_hash
      end

      def reset(params, scope, options)
        validator = Fluent::Counter::ArrayValidator.new(:empty, :key)
        valid_params, errors = validator.call(params)
        res = Response.new(errors)

        do_reset = lambda do |store, key|
          begin
            v = store.reset(key, scope)
            res.push_data v
          rescue => e
            res.push_error e
          end
        end

        if options['random']
          @mutex_hash.synchronize_keys(*valid_params, &do_reset)
        else
          @mutex_hash.synchronize(*valid_params, &do_reset)
        end

        res.to_hash
      end

      def get(params, scope, _options)
        validator = Fluent::Counter::ArrayValidator.new(:empty, :key)
        valid_params, errors = validator.call(params)
        res = Response.new(errors)

        valid_params.each do |key|
          begin
            v = @store.get(key, scope, raise_error: true)
            res.push_data v
          rescue => e
            res.push_error e
          end
        end
        res.to_hash
      end

      def safe_run
        yield
      rescue => e
        {
          'errors' => [InternalServerError.new(e).to_hash],
          'data' => []
        }
      end

      class Response
        def initialize(errors = [], data = [])
          @errors = errors
          @data = data
        end

        def push_error(error)
          @errors << error
        end

        def push_data(data)
          @data << data
        end

        def to_hash
          data = @data.map { |d| d.respond_to?(:to_response_hash) ? d.to_response_hash : d }

          if @errors.empty?
            { 'data' => data }
          else
            errors = @errors.map do |e|
              error = e.respond_to?(:to_hash) ? e : InternalServerError.new(e.to_s)
              error.to_hash
            end
            { 'data' => data, 'errors' => errors }
          end
        end
      end
    end

    class Handler < Fluent::Counter::BaseSocket
      def initialize(io, on_message)
        super(io)
        @on_message = on_message
      end

      def on_message(data)
        res = @on_message.call(data)
        packed_write res
      rescue => e
        puts "server #{e}"
      end
    end
  end
end
