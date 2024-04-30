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

require 'net/http'
require 'uri'
require 'openssl'
require 'fluent/tls'
require 'fluent/plugin/output'
require 'fluent/plugin_helper/socket'

# patch Net::HTTP to support extra_chain_cert which was added in Ruby feature #9758.
# see: https://github.com/ruby/ruby/commit/31af0dafba6d3769d2a39617c0dddedb97883712
unless Net::HTTP::SSL_IVNAMES.include?(:@extra_chain_cert)
  class Net::HTTP
    SSL_IVNAMES << :@extra_chain_cert
    SSL_ATTRIBUTES << :extra_chain_cert
    attr_accessor :extra_chain_cert
  end
end

module Fluent::Plugin
  class HTTPOutput < Output
    Fluent::Plugin.register_output('http', self)

    class RetryableResponse < StandardError; end

    ConnectionCache = Struct.new(:uri, :conn)

    helpers :formatter

    desc 'The endpoint for HTTP request, e.g. http://example.com/api'
    config_param :endpoint, :string
    desc 'The method for HTTP request'
    config_param :http_method, :enum, list: [:put, :post], default: :post
    desc 'The proxy for HTTP request'
    config_param :proxy, :string, default: ENV['HTTP_PROXY'] || ENV['http_proxy']
    desc 'Content-Type for HTTP request'
    config_param :content_type, :string, default: nil
    desc 'JSON array data format for HTTP request body'
    config_param :json_array, :bool, default: false
    desc 'Additional headers for HTTP request'
    config_param :headers, :hash, default: nil
    desc 'Additional placeholder based headers for HTTP request'
    config_param :headers_from_placeholders, :hash, default: nil

    desc 'The connection open timeout in seconds'
    config_param :open_timeout, :integer, default: nil
    desc 'The read timeout in seconds'
    config_param :read_timeout, :integer, default: nil
    desc 'The TLS timeout in seconds'
    config_param :ssl_timeout, :integer, default: nil
    desc 'Try to reuse connections'
    config_param :reuse_connections, :bool, default: false

    desc 'The CA certificate path for TLS'
    config_param :tls_ca_cert_path, :string, default: nil
    desc 'The client certificate path for TLS'
    config_param :tls_client_cert_path, :string, default: nil
    desc 'The client private key path for TLS'
    config_param :tls_private_key_path, :string, default: nil
    desc 'The client private key passphrase for TLS'
    config_param :tls_private_key_passphrase, :string, default: nil, secret: true
    desc 'The verify mode of TLS'
    config_param :tls_verify_mode, :enum, list: [:none, :peer], default: :peer
    desc 'The default version of TLS'
    config_param :tls_version, :enum, list: Fluent::TLS::SUPPORTED_VERSIONS, default: Fluent::TLS::DEFAULT_VERSION
    desc 'The cipher configuration of TLS'
    config_param :tls_ciphers, :string, default: Fluent::TLS::CIPHERS_DEFAULT

    desc 'Raise UnrecoverableError when the response is non success, 4xx/5xx'
    config_param :error_response_as_unrecoverable, :bool, default: true
    desc 'The list of retryable response code'
    config_param :retryable_response_codes, :array, value_type: :integer, default: nil

    config_section :format do
      config_set_default :@type, 'json'
    end

    config_section :auth, required: false, multi: false do
      desc 'The method for HTTP authentication'
      config_param :method, :enum, list: [:basic, :aws_sigv4], default: :basic
      desc 'The username for basic authentication'
      config_param :username, :string, default: nil
      desc 'The password for basic authentication'
      config_param :password, :string, default: nil, secret: true
      desc 'The AWS service to authenticate against'
      config_param :aws_service, :string, default: nil
      desc 'The AWS region to use when authenticating'
      config_param :aws_region, :string, default: nil
      desc 'The AWS role ARN to assume when authenticating'
      config_param :aws_role_arn, :string, default: nil
    end

    def connection_cache_id_thread_key
      "#{plugin_id}_connection_cache_id"
    end

    def connection_cache_id_for_thread
      Thread.current[connection_cache_id_thread_key]
    end

    def connection_cache_id_for_thread=(id)
      Thread.current[connection_cache_id_thread_key] = id
    end

    def initialize
      super

      @uri = nil
      @proxy_uri = nil
      @formatter = nil

      @connection_cache = []
      @connection_cache_id_mutex = Mutex.new
      @connection_cache_next_id = 0
    end

    def close
      super

      @connection_cache.each {|entry| entry.conn.finish if entry.conn&.started? }
    end

    def configure(conf)
      super

      @connection_cache = Array.new(actual_flush_thread_count, ConnectionCache.new("", nil)) if @reuse_connections

      if @retryable_response_codes.nil?
        log.warn('Status code 503 is going to be removed from default `retryable_response_codes` from fluentd v2. Please add it by yourself if you wish')
        @retryable_response_codes = [503]
      end

      @http_opt = setup_http_option
      @proxy_uri = URI.parse(@proxy) if @proxy
      @formatter = formatter_create
      @content_type = setup_content_type unless @content_type

      if @json_array
        if @formatter_configs.first[:@type] != "json"
          raise Fluent::ConfigError, "json_array option could be used with json formatter only"
        end
        define_singleton_method(:format, method(:format_json_array))
      end

      if @auth and @auth.method == :aws_sigv4
        begin
          require 'aws-sigv4'
          require 'aws-sdk-core'
        rescue LoadError
          raise Fluent::ConfigError, "The aws-sdk-core and aws-sigv4 gems are required for aws_sigv4 auth. Run: gem install aws-sdk-core -v '~> 3.191'"
        end

        raise Fluent::ConfigError, "aws_service is required for aws_sigv4 auth" unless @auth.aws_service != nil
        raise Fluent::ConfigError, "aws_region is required for aws_sigv4 auth" unless @auth.aws_region != nil

        if @auth.aws_role_arn == nil
          aws_credentials = Aws::CredentialProviderChain.new.resolve
        else
          aws_credentials = Aws::AssumeRoleCredentials.new(
            client: Aws::STS::Client.new(
              region: @auth.aws_region
            ),
            role_arn: @auth.aws_role_arn,
            role_session_name: "fluentd"
          )
        end

        @aws_signer = Aws::Sigv4::Signer.new(
          service: @auth.aws_service,
          region: @auth.aws_region,
          credentials_provider: aws_credentials
        )
      end
    end

    def multi_workers_ready?
      true
    end

    def formatted_to_msgpack_binary?
      @formatter_configs.first[:@type] == 'msgpack'
    end

    def format(tag, time, record)
      @formatter.format(tag, time, record)
    end

    def format_json_array(tag, time, record)
      @formatter.format(tag, time, record) << ","
    end

    def write(chunk)
      uri = parse_endpoint(chunk)
      req = create_request(chunk, uri)

      log.debug { "#{@http_method.capitalize} data to #{uri.to_s} with chunk(#{dump_unique_id_hex(chunk.unique_id)})" }

      send_request(uri, req)
    end

    private

    def setup_content_type
      case @formatter_configs.first[:@type]
      when 'json'
        @json_array ? 'application/json' : 'application/x-ndjson'
      when 'csv'
        'text/csv'
      when 'tsv', 'ltsv'
        'text/tab-separated-values'
      when 'msgpack'
        'application/x-msgpack'
      when 'out_file', 'single_value', 'stdout', 'hash'
        'text/plain'
      else
        raise Fluent::ConfigError, "can't determine Content-Type from formatter type. Set content_type parameter explicitly"
      end
    end

    def setup_http_option
      use_ssl = @endpoint.start_with?('https')
      opt = {
        open_timeout: @open_timeout,
        read_timeout: @read_timeout,
        ssl_timeout: @ssl_timeout,
        use_ssl: use_ssl
      }

      if use_ssl
        if @tls_ca_cert_path
          raise Fluent::ConfigError, "tls_ca_cert_path is wrong: #{@tls_ca_cert_path}" unless File.file?(@tls_ca_cert_path)
          opt[:ca_file] = @tls_ca_cert_path
        end
        if @tls_client_cert_path
          raise Fluent::ConfigError, "tls_client_cert_path is wrong: #{@tls_client_cert_path}" unless File.file?(@tls_client_cert_path)

          bundle = File.read(@tls_client_cert_path)
          bundle_certs = bundle.scan(/-----BEGIN CERTIFICATE-----(?:.|\n)+?-----END CERTIFICATE-----/)
          opt[:cert] = OpenSSL::X509::Certificate.new(bundle_certs[0])

          intermediate_certs = bundle_certs[1..-1]
          if intermediate_certs
            opt[:extra_chain_cert] = intermediate_certs.map { |cert| OpenSSL::X509::Certificate.new(cert) }
          end
        end
        if @tls_private_key_path
          raise Fluent::ConfigError, "tls_private_key_path is wrong: #{@tls_private_key_path}" unless File.file?(@tls_private_key_path)
          opt[:key] = OpenSSL::PKey.read(File.read(@tls_private_key_path), @tls_private_key_passphrase)
        end
        opt[:verify_mode] = case @tls_verify_mode
                            when :none
                              OpenSSL::SSL::VERIFY_NONE
                            when :peer
                              OpenSSL::SSL::VERIFY_PEER
                            end
        opt[:ciphers] = @tls_ciphers
        opt[:ssl_version] = @tls_version
      end

      opt
    end

    def parse_endpoint(chunk)
      endpoint = extract_placeholders(@endpoint, chunk)
      URI.parse(endpoint)
    end

    def set_headers(req, uri, chunk)
      if @headers
        @headers.each do |k, v|
          req[k] = v
        end
      end
      if @headers_from_placeholders
        @headers_from_placeholders.each do |k, v|
          req[k] = extract_placeholders(v, chunk)
        end
      end
      req['Content-Type'] = @content_type
    end

    def set_auth(req, uri)
      return unless @auth

      if @auth.method == :basic
        req.basic_auth(@auth.username, @auth.password)
      elsif @auth.method == :aws_sigv4
        signature = @aws_signer.sign_request(
          http_method: req.method,
          url: uri.request_uri,
          headers: {
            'Content-Type' => @content_type,
            'Host' => uri.host
          },
          body: req.body
        )
        req.add_field('x-amz-date', signature.headers['x-amz-date'])
        req.add_field('x-amz-security-token', signature.headers['x-amz-security-token'])
        req.add_field('x-amz-content-sha256', signature.headers['x-amz-content-sha256'])
        req.add_field('authorization', signature.headers['authorization'])
      end
    end

    def create_request(chunk, uri)
      req = case @http_method
            when :post
              Net::HTTP::Post.new(uri.request_uri)
            when :put
              Net::HTTP::Put.new(uri.request_uri)
            end
      set_headers(req, uri, chunk)
      req.body = @json_array ? "[#{chunk.read.chop}]" : chunk.read

      # At least one authentication method requires the body and other headers, so the order of this call matters
      set_auth(req, uri)
      req
    end

    def make_request_cached(uri, req)
      id = self.connection_cache_id_for_thread
      if id.nil?
        @connection_cache_id_mutex.synchronize {
          id = @connection_cache_next_id
          @connection_cache_next_id += 1
        }
        self.connection_cache_id_for_thread = id
      end
      uri_str = uri.to_s
      if @connection_cache[id].uri != uri_str
        @connection_cache[id].conn.finish if @connection_cache[id].conn&.started?
        http =  if @proxy_uri
                  Net::HTTP.start(uri.host, uri.port, @proxy_uri.host, @proxy_uri.port, @proxy_uri.user, @proxy_uri.password, @http_opt)
                else
                  Net::HTTP.start(uri.host, uri.port, @http_opt)
                end
        @connection_cache[id] = ConnectionCache.new(uri_str, http)
      end
      @connection_cache[id].conn.request(req)
    end

    def make_request(uri, req, &block)
      if @proxy_uri
        Net::HTTP.start(uri.host, uri.port, @proxy_uri.host, @proxy_uri.port, @proxy_uri.user, @proxy_uri.password, @http_opt, &block)
      else
        Net::HTTP.start(uri.host, uri.port, @http_opt, &block)
      end
    end

    def send_request(uri, req)
      res = if @reuse_connections
              make_request_cached(uri, req)
            else
              make_request(uri, req) { |http| http.request(req) }
            end

      if res.is_a?(Net::HTTPSuccess)
        log.debug { "#{res.code} #{res.message.rstrip}#{res.body.lstrip}" }
      else
        msg = "#{res.code} #{res.message.rstrip} #{res.body.lstrip}"

        if @retryable_response_codes.include?(res.code.to_i)
          raise RetryableResponse, msg
        end

        if @error_response_as_unrecoverable
          raise Fluent::UnrecoverableError, msg
        else
          log.error "got error response from '#{@http_method.capitalize} #{uri.to_s}' : #{msg}"
        end
      end
    end
  end
end
