#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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
module Fluentd

  require 'drb'
  require 'drb/unix'

  class MultiProcessManager < ProcessManager
    def initialize
      super

      # overwrite socket_manager
      @socket_manager = SocketManager::Server.new
    end

    # override
    def logger=(logger)
      @socket_manager.logger = logger
      @logger = logger
    end

    # override
    def assign_processors(root_agent)
      pcmap = {}

      assign(pcmap, root_agent, ['default'])

      @processors = pcmap.values
    end

    def assign(pcmap, agent, parent_groups)
      groups = agent.process_groups
      groups = parent_groups if groups.empty?

      pcs = groups.map {|name|
        pcmap[name] ||= MultiProcessProcessor.new(name)
      }

      if pcs.size == 1
        assign_single(pcs[0], agent)
      else
        assign_multi(pcs, agent)
      end

      agent.agents.each {|a|
        assign(pcmap, a, groups)
      }
    end

    def assign_single(pc, agent)
      agent.extend(CollectorProxy)
      agent.setup_collector_proxy!(pc)

      pc.assign_agent(agent)
    end

    def assign_multi(pcs, agent)
      agent.extend(RoundRobinCollectorProxy)
      agent.setup_collector_proxy!(pcs)

      pcs.each {|pc|
        pc.assign_agent(agent)
      }
    end

    def before_run
      @socket_manager.start
    end

    def after_run
      @socket_manager.shutdown
    end

    class MultiProcessProcessor < ProcessManager::Processor
      def initialize(name)
        @name = name
        @ipx = InterProcessExchange.new(name)
        super()
      end

      def before_fork
        @smc = process_manager.socket_manager.new_client
      end

      def run
        process_manager.socket_manager.close_client_connections

        # overwrite socket_manager_api set by Server#initialize
        Fluentd.socket_manager_api = @smc

        @ipx.start
        begin
          super
        ensure
          @ipx.stop
        end

      ensure
        @smc.close
      end

      def after_start
        @smc.close
      end

      def remote_self(target)
        # called by remote process
        @ipx.remote_self(target)
      end
    end

    module CollectorProxy
      def setup_collector_proxy!(processor)
        @processor = processor
      end

      def emits(tag, es)
        if @processor.local_process?
          return super
        end

        @processor.remote_self(self).emits(tag, es)
      end

      def short_circuit(tag)
        if @processor.local_process?
          return super
        end

        @processor.remote_self(self).short_circuit(tag)
      end
    end

    module RoundRobinCollectorProxy
      def setup_collector_proxy!(processors)
        @processors = processors
        @processor_rr = 0
      end

      def emits(tag, es)
        if @processors.find_if {|pc| pc.local_process? }
          return super
        end

        rr = @processor_rr = (@processor_rr + 1) % @processors.size
        pc = @processors[rr]

        pc.remote_self(self).emits(tag, es)
      end

      # TODO short_circuit match cache
    end
  end

  class InterProcessExchange
    def initialize(name)
      tmpdir = Dir.tmpdir
      @path = File.join(tmpdir, "fluentd", "processor.#{name}.#{Process.pid}")
      @drb_uri = "drbunix:#{@path}"
    end

    def close
      File.unlink(@path) rescue nil
    end

    class DRbFrontObject
      def ref_id(self_id)
        ObjectSpace._id2ref(self_id)
      end
    end

    def start
      FileUtils.mkdir_p File.dirname(@path)
      File.unlink(@path) rescue nil
      @server = DRb::DRbServer.new(@drb_uri, DRbFrontObject.new)
    end

    def join
      @server.thread.join
    end

    def stop
      @server.stop_service if @server
    end

    def remote_self(local_self)
      remote = DRb::DRbObject.new_with_uri(@drb_uri)
      remote.ref_id(local_self.__id__)
    end
  end

end
