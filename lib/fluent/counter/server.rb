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
      DEFAULT_ADDR = '127.0.0.1'
      DEFAULT_PORT = 24321

      def initialize(name, opt = {})
        raise 'Counter server name is invalid' unless Validator::VALID_NAME =~ name
        @name = name
        @opt = opt
        @addr = @opt[:addr] || DEFAULT_ADDR
        @port = @opt[:port] || DEFAULT_PORT
        @loop = @opt[:loop] || Coolio::Loop.new
        @log =  @opt[:log] || $log

        @store = Fluent::Counter::Store.new(opt)
        @mutex_hash = MutexHash.new(@store)

        @server = Coolio::TCPServer.new(@addr, @port, Handler, method(:on_message))
        @thread = nil
        @running = false
      end

      def start
        @server.attach(@loop)
        @thread = Thread.new do
          @running = true
          @loop.run(0.5)
          @running = false
        end
        @log.debug("starting counter server #{@addr}:#{@port}")
        @mutex_hash.start
        self
      end

      def stop
        # This `sleep` for a test to wait for a `@loop` to begin to run
        sleep 0.1
        @server.close
        @loop.stop if @running
        @mutex_hash.stop
        @thread.join if @thread
        @log.debug("calling stop in counter server #{@addr}:#{@port}")
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
      rescue => e
        @log.error e.to_s
      end

      private

      def establish(params, _scope, _options)
        validator = Fluent::Counter::ArrayValidator.new(:empty, :scope)
        valid_params, errors = validator.call(params)
        res = Response.new(errors)

        if scope = valid_params.first
          new_scope = "#{@name}\t#{scope}"
          res.push_data new_scope
          @log.debug("Establish new key: #{new_scope}")
        end

        res.to_hash
      end

      def init(params, scope, options)
        validator = Fluent::Counter::HashValidator.new(:empty, :name, :reset_interval)
        valid_params, errors = validator.call(params)
        res = Response.new(errors)
        key_hash = valid_params.reduce({}) do |acc, vp|
          acc.merge(Store.gen_key(scope, vp['name']) => vp)
        end

        do_init = lambda do |store, key|
          begin
            param = key_hash[key]
            v = store.init(key, param, ignore: options['ignore'])
            @log.debug("Create new key: #{param['name']}")
            res.push_data v
          rescue => e
            res.push_error e
          end
        end

        if options['random']
          @mutex_hash.synchronize_keys(*(key_hash.keys), &do_init)
        else
          @mutex_hash.synchronize(*(key_hash.keys), &do_init)
        end

        res.to_hash
      end

      def delete(params, scope, options)
        validator = Fluent::Counter::ArrayValidator.new(:empty, :key)
        valid_params, errors = validator.call(params)
        res = Response.new(errors)
        keys = valid_params.map { |vp| Store.gen_key(scope, vp) }

        do_delete = lambda do |store, key|
          begin
            v = store.delete(key)
            @log.debug("delete a key: #{key}")
            res.push_data v
          rescue => e
            res.push_error e
          end
        end

        if options['random']
          @mutex_hash.synchronize_keys(*keys, &do_delete)
        else
          @mutex_hash.synchronize(*keys, &do_delete)
        end

        res.to_hash
      end

      def inc(params, scope, options)
        validate_param = [:empty, :name, :value]
        validate_param << :reset_interval if options['force']
        validator = Fluent::Counter::HashValidator.new(*validate_param)
        valid_params, errors = validator.call(params)
        res = Response.new(errors)
        key_hash = valid_params.reduce({}) do |acc, vp|
          acc.merge(Store.gen_key(scope, vp['name']) => vp)
        end

        do_inc = lambda do |store, key|
          begin
            param = key_hash[key]
            v = store.inc(key, param, force: options['force'])
            @log.debug("Increment #{key} by #{param['value']}")
            res.push_data v
          rescue => e
            res.push_error e
          end
        end

        if options['random']
          @mutex_hash.synchronize_keys(*(key_hash.keys), &do_inc)
        else
          @mutex_hash.synchronize(*(key_hash.keys), &do_inc)
        end

        res.to_hash
      end

      def reset(params, scope, options)
        validator = Fluent::Counter::ArrayValidator.new(:empty, :key)
        valid_params, errors = validator.call(params)
        res = Response.new(errors)
        keys = valid_params.map { |vp| Store.gen_key(scope, vp) }

        do_reset = lambda do |store, key|
          begin
            v = store.reset(key)
            @log.debug("Reset #{key}'s' counter value")
            res.push_data v
          rescue => e
            res.push_error e
          end
        end

        if options['random']
          @mutex_hash.synchronize_keys(*keys, &do_reset)
        else
          @mutex_hash.synchronize(*keys, &do_reset)
        end

        res.to_hash
      end

      def get(params, scope, _options)
        validator = Fluent::Counter::ArrayValidator.new(:empty, :key)
        valid_params, errors = validator.call(params)
        res = Response.new(errors)

        keys = valid_params.map { |vp| Store.gen_key(scope, vp) }
        keys.each do |key|
          begin
            v = @store.get(key, raise_error: true)
            @log.debug("Get counter value: #{key}")
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
          if @errors.empty?
            { 'data' => @data }
          else
            errors = @errors.map do |e|
              error = e.respond_to?(:to_hash) ? e : InternalServerError.new(e.to_s)
              error.to_hash
            end
            { 'data' => @data, 'errors' => errors }
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
        packed_write res if res
      end
    end
  end
end
