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

require 'fluent/config/error'

module Fluent
  class Daemonizer
    def self.daemonize(pid_path, args = [], &block)
      new.daemonize(pid_path, args, &block)
    end

    def daemonize(pid_path, args = [])
      pid_fullpath = File.absolute_path(pid_path)
      check_pidfile(pid_fullpath)

      begin
        Process.daemon(false, false)

        File.write(pid_fullpath, Process.pid.to_s)

        # install signal and set process name are performed by supervisor
        install_at_exit_handlers(pid_fullpath)

        yield
      rescue NotImplementedError
        daemonize_with_spawn(pid_fullpath, args)
      end
    end

    private

    def daemonize_with_spawn(pid_fullpath, args)
      pid = Process.spawn(*['fluentd'].concat(args))

      File.write(pid_fullpath, pid.to_s)

      pid
    end

    def check_pidfile(pid_path)
      if File.exist?(pid_path)
        if !File.readable?(pid_path) || !File.writable?(pid_path)
          raise Fluent::ConfigError, "Cannot access pid file: #{pid_path}"
        end

        pid =
          begin
            Integer(File.read(pid_path), 10)
          rescue TypeError, ArgumentError
            return # ignore
          end

        begin
          Process.kill(0, pid)
          raise Fluent::ConfigError, "pid(#{pid}) is running"
        rescue Errno::EPERM
          raise Fluent::ConfigError, "pid(#{pid}) is running"
        rescue Errno::ESRCH
        end
      else
        unless File.writable?(File.dirname(pid_path))
          raise Fluent::ConfigError, "Cannot access directory for pid file: #{File.dirname(pid_path)}"
        end
      end
    end

    def install_at_exit_handlers(pidfile)
      at_exit do
        if File.exist?(pidfile)
          File.delete(pidfile)
        end
      end
    end
  end
end
