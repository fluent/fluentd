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

module Fluent::Plugin
  class HTTPOutput < Output
    Fluent::Plugin.register_output('http', self)

    class RetryableResponse < StandardError; end

    helpers :formatter

    desc 'The endpoint for HTTP request, e.g. http://example.com/api'
    config_param :endpoint, :string
    desc 'The method for HTTP request'
    config_param :http_method, :enum, list: [:put, :post], default: :post
    desc 'The proxy for HTTP request'
    config_param :proxy, :string, default: ENV['HTTP_PROXY'] || ENV['http_proxy']
    desc 'Content-Type for HTTP request'
    config_param :content_type, :string, default: nil
    desc 'Additional headers for HTTP request'
    config_param :headers, :hash, default: nil

    desc 'The connection open timeout in seconds'
    config_param :open_timeout, :integer, default: nil
    desc 'The read timeout in seconds'
    config_param :read_timeout, :integer, default: nil
    desc 'The TLS timeout in seconds'
    config_param :ssl_timeout, :integer, default: nil

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
      config_param :method, :enum, list: [:basic], default: :basic
      desc 'The username for basic authentication'
      config_param :username, :string, default: nil
      desc 'The password for basic authentication'
      config_param :password, :string, default: nil, secret: true
    end

    def initialize
      super

      @uri = nil
      @proxy_uri = nil
      @formatter = nil
    end

    def configure(conf)
      super

      if @retryable_response_codes.nil?
        log.warn('Status code 503 is going to be removed from default `retryable_response_codes` from fluentd v2. Please add it by yourself if you wish')
        @retryable_response_codes = [503]
      end

      @http_opt = setup_http_option
      @proxy_uri = URI.parse(@proxy) if @proxy
      @formatter = formatter_create
      @content_type = setup_content_type unless @content_type
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
        'application/x-ndjson'
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
          opt[:cert] = OpenSSL::X509::Certificate.new(File.read(@tls_client_cert_path))
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

    def set_headers(req)
      if @headers
        @headers.each do |k, v|
          req[k] = v
        end
      end
      req['Content-Type'] = @content_type
    end

    def create_request(chunk, uri)
      req = case @http_method
            when :post
              Net::HTTP::Post.new(uri.request_uri)
            when :put
              Net::HTTP::Put.new(uri.request_uri)
            end
      if @auth
        req.basic_auth(@auth.username, @auth.password)
      end
      set_headers(req)
      req.body = chunk.read
      req
    end

    def send_request(uri, req)
      res = if @proxy_uri
              Net::HTTP.start(uri.host, uri.port, @proxy_uri.host, @proxy_uri.port, @proxy_uri.user, @proxy_uri.password, @http_opt) { |http|
                http.request(req)
              }
            else
              Net::HTTP.start(uri.host, uri.port, @http_opt) { |http|
                http.request(req)
              }
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
