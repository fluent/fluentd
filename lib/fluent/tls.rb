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

require 'openssl'
require 'fluent/config/error'

module Fluent
  module TLS
    DEFAULT_VERSION = :'TLSv1_2'
    SUPPORTED_VERSIONS = if defined?(OpenSSL::SSL::TLS1_3_VERSION)
                           [:'TLSv1_1', :'TLSv1_2', :'TLSv1_3', :'TLS1_1', :'TLS1_2', :'TLS1_3'].freeze
                         else
                           [:'TLSv1_1', :'TLSv1_2', :'TLS1_1', :'TLS1_2'].freeze
                         end
    ### follow httpclient configuration by nahi
    # OpenSSL 0.9.8 default: "ALL:!ADH:!LOW:!EXP:!MD5:+SSLv2:@STRENGTH"
    CIPHERS_DEFAULT = "ALL:!aNULL:!eNULL:!SSLv2".freeze # OpenSSL >1.0.0 default

    METHODS_MAP = begin
                    map = {
                      TLSv1: OpenSSL::SSL::TLS1_VERSION,
                      TLSv1_1: OpenSSL::SSL::TLS1_1_VERSION,
                      TLSv1_2: OpenSSL::SSL::TLS1_2_VERSION
                    }
                    map[:'TLSv1_3'] = OpenSSL::SSL::TLS1_3_VERSION if defined?(OpenSSL::SSL::TLS1_3_VERSION)
                    MIN_MAX_AVAILABLE = true
                    map.freeze
                  rescue NameError
                    # ruby 2.4 doesn't have OpenSSL::SSL::TLSXXX constants and min_version=/max_version= methods
                    map = {
                      TLS1: :'TLSv1',
                      TLS1_1: :'TLSv1_1',
                      TLS1_2: :'TLSv1_2',
                    }.freeze
                    MIN_MAX_AVAILABLE = false
                    map
                  end
    private_constant :METHODS_MAP

    # Helper for old syntax/method support:
    # ruby 2.4 uses ssl_version= but this method is now deprecated.
    # min_version=/max_version= use 'TLS1_2' but ssl_version= uses 'TLSv1_2'
    def set_version_to_context(ctx, version, min_version, max_version)
      if MIN_MAX_AVAILABLE
        case
        when min_version.nil? && max_version.nil?
          min_version = METHODS_MAP[version] || version
          max_version = METHODS_MAP[version] || version
        when min_version.nil? && max_version
          raise Fluent::ConfigError, "When you set max_version, must set min_version together"
        when min_version && max_version.nil?
          raise Fluent::ConfigError, "When you set min_version, must set max_version together"
        else
          min_version = METHODS_MAP[min_version] || min_version
          max_version = METHODS_MAP[max_version] || max_version
        end
        ctx.min_version = min_version
        ctx.max_version = max_version
      else
        ctx.ssl_version = METHODS_MAP[version] || version
      end

      ctx
    end
    module_function :set_version_to_context

    def set_version_to_options(opt, version, min_version, max_version)
      if MIN_MAX_AVAILABLE
        case
        when min_version.nil? && max_version.nil?
          min_version = METHODS_MAP[version] || version
          max_version = METHODS_MAP[version] || version
        when min_version.nil? && max_version
          raise Fluent::ConfigError, "When you set max_version, must set min_version together"
        when min_version && max_version.nil?
          raise Fluent::ConfigError, "When you set min_version, must set max_version together"
        else
          min_version = METHODS_MAP[min_version] || min_version
          max_version = METHODS_MAP[max_version] || max_version
        end
        opt[:min_version] = min_version
        opt[:max_version] = max_version
      else
        opt[:ssl_version] = METHODS_MAP[version] || version
      end

      opt
    end
    module_function :set_version_to_options
  end
end

