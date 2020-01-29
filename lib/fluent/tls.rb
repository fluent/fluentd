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
    SUPPORTED_VERSIONS = [:'TLSv1_1', :'TLSv1_2', :'TLS1_1', :'TLS1_2']
    ### follow httpclient configuration by nahi
    # OpenSSL 0.9.8 default: "ALL:!ADH:!LOW:!EXP:!MD5:+SSLv2:@STRENGTH"
    CIPHERS_DEFAULT = "ALL:!aNULL:!eNULL:!SSLv2" # OpenSSL >1.0.0 default

    METHODS_MAP = {
      TLSv1: OpenSSL::SSL::TLS1_VERSION,
      TLSv1_1: OpenSSL::SSL::TLS1_1_VERSION,
      TLSv1_2: OpenSSL::SSL::TLS1_2_VERSION,
    }.freeze
    METHODS_MAP_FOR_VERSION = {
      TLS1: :'TLSv1',
      TLS1_1: :'TLSv1_1',
      TLS1_2: :'TLSv1_2',
    }.freeze

    # Helper for old syntax/method support:
    # ruby 2.4 uses ssl_version= but this method is now deprecated.
    # min_version=/max_version= use 'TLS1_2' but ssl_version= uses 'TLSv1_2'
    def set_version_to_context(ctx, version, min_version, max_version)
      if ctx.respond_to?(:'min_version=')
        case
        when min_version.nil? && max_version.nil?
          min_version = METHODS_MAP[version]
          max_version = METHODS_MAP[version]
        when min_version.nil? && max_version
          raise Fluentd::ConfigError, "When you set max_version, must set min_version together"
        when min_version && max_version.nil?
          raise Fluentd::ConfigError, "When you set min_version, must set max_version together"
        else
          min_version = METHODS_MAP[min_version] || min_version
          max_version = METHODS_MAP[max_version] || max_version
        end
        ctx.min_version = min_version
        ctx.max_version = max_version
      else
        ctx.ssl_version = METHODS_MAP_FOR_VERSION[version] || version
      end

      ctx
    end

    module_function :set_version_to_context
  end
end

