require_relative 'helper'

class SourceOnlyBufferAgentTest < ::Test::Unit::TestCase
  def log
    logger = ServerEngine::DaemonLogger.new(
      Fluent::Test::DummyLogDevice.new,
      { log_level: ServerEngine::DaemonLogger::INFO }
    )
    Fluent::Log.new(logger)
  end

  def setup
    omit "Not supported on Windows" if Fluent.windows?
    @log = log
  end

  sub_test_case "#configure" do
    test "default" do
      system_config = Fluent::SystemConfig.new
      root_agent = Fluent::RootAgent.new(log: @log, system_config: system_config)
      stub(Fluent::Engine).root_agent { root_agent }
      stub(Fluent::Engine).system_config { system_config }
      root_agent.configure(config_element)

      agent = Fluent::SourceOnlyBufferAgent.new(log: @log, system_config: system_config)
      agent.configure

      assert_equal(
        {
          "num of filter plugins" => 0,
          "num of output plugins" => 1,
          "base_buffer_dir" => agent.instance_variable_get(:@default_buffer_path),
          "actual_buffer_dir" => agent.instance_variable_get(:@default_buffer_path),
          "EventRouter of BufferOutput" => root_agent.event_router.object_id,
          "flush_thread_count" => 0,
          "flush_at_shutdown" => false,
        },
        {
          "num of filter plugins" => agent.filters.size,
          "num of output plugins" => agent.outputs.size,
          "base_buffer_dir" => agent.instance_variable_get(:@base_buffer_dir),
          "actual_buffer_dir" => agent.instance_variable_get(:@actual_buffer_dir),
          "EventRouter of BufferOutput" => agent.outputs[0].router.object_id,
          "flush_thread_count" => agent.outputs[0].buffer_config.flush_thread_count,
          "flush_at_shutdown" => agent.outputs[0].buffer_config.flush_at_shutdown,
        }
      )

      assert do
        @log.out.logs.any? { |log| log.include? "the emitted data will be stored in the buffer files" }
      end
    end

    test "flush: true" do
      system_config = Fluent::SystemConfig.new
      root_agent = Fluent::RootAgent.new(log: @log, system_config: system_config)
      stub(Fluent::Engine).root_agent { root_agent }
      stub(Fluent::Engine).system_config { system_config }
      root_agent.configure(config_element)

      agent = Fluent::SourceOnlyBufferAgent.new(log: @log, system_config: system_config)
      agent.configure(flush: true)

      assert_equal(
        {
          "num of filter plugins" => 0,
          "num of output plugins" => 1,
          "base_buffer_dir" => agent.instance_variable_get(:@default_buffer_path),
          "actual_buffer_dir" => agent.instance_variable_get(:@default_buffer_path),
          "EventRouter of BufferOutput" => root_agent.event_router.object_id,
          "flush_thread_count" => 1,
          "flush_at_shutdown" => true,
        },
        {
          "num of filter plugins" => agent.filters.size,
          "num of output plugins" => agent.outputs.size,
          "base_buffer_dir" => agent.instance_variable_get(:@base_buffer_dir),
          "actual_buffer_dir" => agent.instance_variable_get(:@actual_buffer_dir),
          "EventRouter of BufferOutput" => agent.outputs[0].router.object_id,
          "flush_thread_count" => agent.outputs[0].buffer_config.flush_thread_count,
          "flush_at_shutdown" => agent.outputs[0].buffer_config.flush_at_shutdown,
        }
      )

      assert do
        not @log.out.logs.any? { |log| log.include? "the emitted data will be stored in the buffer files" }
      end
    end

    test "multiple workers" do
      system_config = Fluent::SystemConfig.new(config_element("system", "", {"workers" => 2}))
      root_agent = Fluent::RootAgent.new(log: @log, system_config: system_config)
      stub(Fluent::Engine).root_agent { root_agent }
      stub(Fluent::Engine).system_config { system_config }
      root_agent.configure(config_element)

      agent = Fluent::SourceOnlyBufferAgent.new(log: @log, system_config: system_config)
      agent.configure

      assert_equal(
        {
          "num of filter plugins" => 0,
          "num of output plugins" => 1,
          "base_buffer_dir" => agent.instance_variable_get(:@default_buffer_path),
          "actual_buffer_dir" => "#{agent.instance_variable_get(:@default_buffer_path)}/worker0",
          "EventRouter of BufferOutput" => root_agent.event_router.object_id,
          "flush_thread_count" => 0,
          "flush_at_shutdown" => false,
        },
        {
          "num of filter plugins" => agent.filters.size,
          "num of output plugins" => agent.outputs.size,
          "base_buffer_dir" => agent.instance_variable_get(:@base_buffer_dir),
          "actual_buffer_dir" => agent.instance_variable_get(:@actual_buffer_dir),
          "EventRouter of BufferOutput" => agent.outputs[0].router.object_id,
          "flush_thread_count" => agent.outputs[0].buffer_config.flush_thread_count,
          "flush_at_shutdown" => agent.outputs[0].buffer_config.flush_at_shutdown,
        }
      )
    end

    test "full setting with flush:true" do
      system_config = Fluent::SystemConfig.new(config_element("system", "", {}, [
        config_element("source_only_buffer", "", {
          "flush_thread_count" => 4,
          "overflow_action" => :throw_exception,
          "path" => "tmp_buffer_path",
          "flush_interval" => 1,
          "chunk_limit_size" => 100,
          "total_limit_size" => 1000,
          "compress" => :gzip,
        })
      ]))
      root_agent = Fluent::RootAgent.new(log: @log, system_config: system_config)
      stub(Fluent::Engine).root_agent { root_agent }
      stub(Fluent::Engine).system_config { system_config }
      root_agent.configure(config_element)

      agent = Fluent::SourceOnlyBufferAgent.new(log: @log, system_config: system_config)
      agent.configure(flush: true)

      assert_equal(
        {
          "num of filter plugins" => 0,
          "num of output plugins" => 1,
          "base_buffer_dir" => "tmp_buffer_path",
          "actual_buffer_dir" => "tmp_buffer_path",
          "EventRouter of BufferOutput" => root_agent.event_router.object_id,
          "flush_thread_count" => 4,
          "flush_at_shutdown" => true,
          "overflow_action" => :throw_exception,
          "flush_interval" => 1,
          "chunk_limit_size" => 100,
          "total_limit_size" => 1000,
          "compress" => :gzip,
        },
        {
          "num of filter plugins" => agent.filters.size,
          "num of output plugins" => agent.outputs.size,
          "base_buffer_dir" => agent.instance_variable_get(:@base_buffer_dir),
          "actual_buffer_dir" => agent.instance_variable_get(:@actual_buffer_dir),
          "EventRouter of BufferOutput" => agent.outputs[0].router.object_id,
          "flush_thread_count" => agent.outputs[0].buffer_config.flush_thread_count,
          "flush_at_shutdown" => agent.outputs[0].buffer_config.flush_at_shutdown,
          "overflow_action" => agent.outputs[0].buffer_config.overflow_action,
          "flush_interval" => agent.outputs[0].buffer_config.flush_interval,
          "chunk_limit_size" => agent.outputs[0].buffer.chunk_limit_size,
          "total_limit_size" => agent.outputs[0].buffer.total_limit_size,
          "compress" => agent.outputs[0].buffer.compress,
        }
      )
    end
  end

  sub_test_case "#cleanup" do
    test "do not remove the buffer if it is not empty" do
      system_config = Fluent::SystemConfig.new
      root_agent = Fluent::RootAgent.new(log: @log, system_config: system_config)
      stub(Fluent::Engine).root_agent { root_agent }
      stub(Fluent::Engine).system_config { system_config }
      root_agent.configure(config_element)

      agent = Fluent::SourceOnlyBufferAgent.new(log: @log, system_config: system_config)
      agent.configure

      stub(Dir).empty?(agent.instance_variable_get(:@actual_buffer_dir)) { false }
      mock(FileUtils).remove_dir.never

      agent.cleanup

      assert do
        @log.out.logs.any? { |log| log.include? "some buffer files remain in" }
      end
    end

    test "remove the buffer if it is empty" do
      system_config = Fluent::SystemConfig.new
      root_agent = Fluent::RootAgent.new(log: @log, system_config: system_config)
      stub(Fluent::Engine).root_agent { root_agent }
      stub(Fluent::Engine).system_config { system_config }
      root_agent.configure(config_element)

      agent = Fluent::SourceOnlyBufferAgent.new(log: @log, system_config: system_config)
      agent.configure

      stub(Dir).empty?(agent.instance_variable_get(:@actual_buffer_dir)) { true }
      mock(FileUtils).remove_dir(agent.instance_variable_get(:@base_buffer_dir)).times(1)

      agent.cleanup

      assert do
        not @log.out.logs.any? { |log| log.include? "some buffer files remain in" }
      end
    end
  end

  sub_test_case "error" do
    test "#emit_error_event" do
      system_config = Fluent::SystemConfig.new
      root_agent = Fluent::RootAgent.new(log: @log, system_config: system_config)
      stub(Fluent::Engine).root_agent { root_agent }
      stub(Fluent::Engine).system_config { system_config }
      root_agent.configure(config_element)

      agent = Fluent::SourceOnlyBufferAgent.new(log: @log, system_config: system_config)
      agent.configure

      agent.event_router.emit_error_event("tag", 0, "hello", Exception.new)

      assert do
        @log.out.logs.any? { |log| log.include? "SourceOnlyBufferAgent: dump an error event" }
      end
    end

    test "#handle_emits_error" do
      system_config = Fluent::SystemConfig.new
      root_agent = Fluent::RootAgent.new(log: @log, system_config: system_config)
      stub(Fluent::Engine).root_agent { root_agent }
      stub(Fluent::Engine).system_config { system_config }
      root_agent.configure(config_element)

      agent = Fluent::SourceOnlyBufferAgent.new(log: @log, system_config: system_config)
      agent.configure

      stub(agent.outputs[0]).emit_events { raise "test error" }

      agent.event_router.emit("foo", 0, "hello")

      assert do
        @log.out.logs.any? { |log| log.include? "SourceOnlyBufferAgent: emit transaction failed" }
      end
    end
  end
end
