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

require 'fluent/plugin'
require 'fluent/configurable'
require 'fluent/system_config'

module Fluent
  module Plugin
    class Base
      include Configurable
      include SystemConfig::Mixin

      State = Struct.new(:configure, :start, :after_start, :stop, :before_shutdown, :shutdown, :after_shutdown, :close, :terminate)

      attr_accessor :under_plugin_development

      def initialize
        @log = nil
        super
        @fluentd_lock_dir = ENV['FLUENTD_LOCK_DIR']
        @_state = State.new(false, false, false, false, false, false, false, false, false)
        @_context_router = nil
        @_fluentd_worker_id = nil
        @under_plugin_development = false
      end

      def has_router?
        false
      end

      def plugin_root_dir
        nil # override this in plugin_id.rb
      end

      def fluentd_worker_id
        return @_fluentd_worker_id if @_fluentd_worker_id
        @_fluentd_worker_id = (ENV['SERVERENGINE_WORKER_ID'] || 0).to_i
        @_fluentd_worker_id
      end

      def configure(conf)
        raise ArgumentError, "BUG: type of conf must be Fluent::Config::Element, but #{conf.class} is passed." unless conf.is_a?(Fluent::Config::Element)

        if conf.for_this_worker? || (Fluent::Engine.supervisor_mode && !conf.for_every_workers?)
          system_config_override(workers: conf.target_worker_ids.size)
        end

        super(conf, system_config.strict_config_value)
        @_state ||= State.new(false, false, false, false, false, false, false, false, false)
        @_state.configure = true
        self
      end

      def multi_workers_ready?
        true
      end

      def get_lock_path(name)
        name = name.gsub(/[^a-zA-Z0-9]/, "_")
        File.join(@fluentd_lock_dir, "fluentd-#{name}.lock")
      end

      def acquire_worker_lock(name)
        if @fluentd_lock_dir.nil?
          raise InvalidLockDirectory, "can't acquire lock because FLUENTD_LOCK_DIR isn't set"
        end
        lock_path = get_lock_path(name)
        File.open(lock_path, "w") do |f|
          f.flock(File::LOCK_EX)
          yield
        end
        # Update access time to prevent tmpwatch from deleting a lock file.
        FileUtils.touch(lock_path)
      end

      def string_safe_encoding(str)
        unless str.valid_encoding?
          str = str.scrub('?')
          log.info "invalid byte sequence is replaced in `#{str}`" if self.respond_to?(:log)
        end
        yield str
      end

      def context_router=(router)
        @_context_router = router
      end

      def context_router
        @_context_router
      end

      def start
        # By initialization order, plugin logger is created before set log_event_enabled.
        # It causes '@id' specified plugin, it uses plugin logger instead of global logger, ignores `<label @FLUENT_LOG>` setting.
        # This is adhoc approach but impact is minimal.
        if @log.is_a?(Fluent::PluginLogger) && $log.respond_to?(:log_event_enabled) # log_event_enabled check for tests
          @log.log_event_enabled = $log.log_event_enabled
        end
        @_state.start = true
        self
      end

      def after_start
        @_state.after_start = true
        self
      end

      def stop
        @_state.stop = true
        self
      end

      def before_shutdown
        @_state.before_shutdown = true
        self
      end

      def shutdown
        @_state.shutdown = true
        self
      end

      def after_shutdown
        @_state.after_shutdown = true
        self
      end

      def close
        @_state.close = true
        self
      end

      def terminate
        @_state.terminate = true
        self
      end

      def configured?
        @_state.configure
      end

      def started?
        @_state.start
      end

      def after_started?
        @_state.after_start
      end

      def stopped?
        @_state.stop
      end

      def before_shutdown?
        @_state.before_shutdown
      end

      def shutdown?
        @_state.shutdown
      end

      def after_shutdown?
        @_state.after_shutdown
      end

      def closed?
        @_state.close
      end

      def terminated?
        @_state.terminate
      end

      def called_in_test?
        caller_locations.each do |location|
          # Thread::Backtrace::Location#path returns base filename or absolute path.
          # #absolute_path returns absolute_path always.
          # https://bugs.ruby-lang.org/issues/12159
          if /\/test_[^\/]+\.rb$/.match?(location.absolute_path) # location.path =~ /test_.+\.rb$/
            return true
          end
        end
        false
      end

      def inspect
        # Plugin instances are sometimes too big to dump because it may have too many thins (buffer,storage, ...)
        # Original commit comment says that:
        #   To emulate normal inspect behavior `ruby -e'o=Object.new;p o;p (o.__id__<<1).to_s(16)'`.
        #   https://github.com/ruby/ruby/blob/trunk/gc.c#L788
        "#<%s:%014x>" % [self.class.name, '0x%014x' % (__id__ << 1)]
      end

      def reloadable_plugin?
        # Engine can't capture all class variables. so it's forbidden to use class variables in each plugins if enabling reload.
        self.class.class_variables.empty?
      end
    end
  end
end
