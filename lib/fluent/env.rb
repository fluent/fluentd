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

require 'securerandom'

require 'serverengine/utils'
require 'fluent/oj_options'

module Fluent
  DEFAULT_CONFIG_PATH = ENV['FLUENT_CONF'] || '/etc/fluent/fluent.conf'
  DEFAULT_PLUGIN_DIR = ENV['FLUENT_PLUGIN'] || '/etc/fluent/plugin'
  DEFAULT_SOCKET_PATH = ENV['FLUENT_SOCKET'] || '/var/run/fluent/fluent.sock'
  DEFAULT_BACKUP_DIR = ENV['FLUENT_BACKUP_DIR'] || '/tmp/fluent'
  DEFAULT_OJ_OPTIONS = Fluent::OjOptions.load_env
  DEFAULT_DIR_PERMISSION = 0755
  DEFAULT_FILE_PERMISSION = 0644
  INSTANCE_ID = ENV['FLUENT_INSTANCE_ID'] || SecureRandom.uuid

  def self.windows?
    ServerEngine.windows?
  end

  def self.linux?
    /linux/ === RUBY_PLATFORM
  end

  def self.macos?
    /darwin/ =~ RUBY_PLATFORM
  end
end
