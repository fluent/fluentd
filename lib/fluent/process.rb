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

require 'fluent/compat/detach_process_mixin'
require 'fluent/env'

module Fluent
  DetachProcessMixin = Fluent::Compat::DetachProcessMixin
  DetachMultiProcessMixin = Fluent::Compat::DetachMultiProcessMixin

  class ProcessDetector
    def self.running_fluentd_conf
      # Detect if same configuration is used
      unless Fluent.windows?
        IO.popen(["/usr/bin/ps", "-e", "-o", "uid,pid,ppid,cmd"]) do |_io|
          _io.readlines.each do |line|
            uid, pid, ppid, cmd = line.split(' ', 4)
            # skip self and supervisor process
            next if Process.pid == pid.to_i or Process.pid == ppid.to_i
            if cmd and cmd.chomp.include?("fluentd") and ppid.to_i == 1
              # under systemd control
              File.open("/proc/#{pid.to_i}/environ") do |file|
                conf = file.read.split("\u0000").select { |entry| entry.include?("FLUENT_CONF") }
                return conf.first.split('=').last unless conf.empty?
              end
            end
          end
        end
      end
      return nil
    end
  end
end
