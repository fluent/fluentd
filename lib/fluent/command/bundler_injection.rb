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

bundle_bin = Gem::Specification.find_by_name('bundler').bin_file('bundle')
ruby_bin = RbConfig.ruby
system("#{ruby_bin} #{bundle_bin} install")
unless $?.success?
  exit $?.exitstatus
end

cmdline = [
  ruby_bin,
  bundle_bin,
  'exec',
  ruby_bin,
  File.expand_path(File.join(File.dirname(__FILE__), 'fluentd.rb')),
] + ARGV

exec *cmdline
exit! 127
