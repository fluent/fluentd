require_relative '../helper'
require 'fluent/plugin/output'
require 'fluent/plugin/buffer'
require 'fluent/event'
require 'fluent/error'

require 'json'
require 'time'
require 'timeout'
require 'timecop'


class BufferedOutputBackupTest < Test::Unit::TestCase
  class BareOutput < Fluent::Plugin::Output
    def register(name, &block)
      instance_variable_set("@#{name}", block)
    end
  end
  class DummyOutput < BareOutput
    def initialize
      super
      @process = nil
      @format = nil
      @write = nil
      @try_write = nil
    end
    def prefer_buffered_processing
      true
    end
    def prefer_delayed_commit
      false
    end
    def process(tag, es)
      @process ? @process.call(tag, es) : nil
    end
    def format(tag, time, record)
      [tag, time.to_i, record].to_json + "\n"
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
    def try_write(chunk)
      @try_write ? @try_write.call(chunk) : nil
    end
  end
  class DummyOutputForSecondary < BareOutput
    def initialize
      super
      @process = nil
      @format = nil
      @write = nil
      @try_write = nil
    end
    def prefer_buffered_processing
      true
    end
    def prefer_delayed_commit
      false
    end
    def process(tag, es)
      @process ? @process.call(tag, es) : nil
    end
    def format(tag, time, record)
      [tag, time.to_i, record].to_json + "\n"
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
    def try_write(chunk)
      @try_write ? @try_write.call(chunk) : nil
    end
  end
  class DummyAsyncOutputForSecondary < BareOutput
    def initialize
      super
      @process = nil
      @format = nil
      @write = nil
      @try_write = nil
    end
    def prefer_buffered_processing
      true
    end
    def prefer_delayed_commit
      true
    end
    def process(tag, es)
      @process ? @process.call(tag, es) : nil
    end
    def format(tag, time, record)
      [tag, time.to_i, record].to_json + "\n"
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
    def try_write(chunk)
      @try_write ? @try_write.call(chunk) : nil
    end
  end

  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/../tmp/bu#{ENV['TEST_ENV_NUMBER']}")

  def create_output
    DummyOutput.new
  end
  def create_metadata(timekey: nil, tag: nil, variables: nil)
    Fluent::Plugin::Buffer::Metadata.new(timekey, tag, variables)
  end
  def waiting(seconds)
    begin
      Timeout.timeout(seconds) do
        yield
      end
    rescue Timeout::Error
      STDERR.print(*@i.log.out.logs)
      raise
    end
  end

  def dummy_event_stream
    Fluent::ArrayEventStream.new([
      [ event_time('2016-04-13 18:33:00'), {"name" => "moris", "age" => 36, "message" => "data1"} ],
      [ event_time('2016-04-13 18:33:13'), {"name" => "moris", "age" => 36, "message" => "data2"} ],
      [ event_time('2016-04-13 18:33:32'), {"name" => "moris", "age" => 36, "message" => "data3"} ],
    ])
  end

  setup do
    @i = create_output
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)

    Fluent::Plugin.register_output('backup_output', DummyOutput)
    Fluent::Plugin.register_output('backup_output2', DummyOutputForSecondary)
    Fluent::Plugin.register_output('backup_async_output', DummyAsyncOutputForSecondary)
  end

  teardown do
    if @i
      @i.stop unless @i.stopped?
      @i.before_shutdown unless @i.before_shutdown?
      @i.shutdown unless @i.shutdown?
      @i.after_shutdown unless @i.after_shutdown?
      @i.close unless @i.closed?
      @i.terminate unless @i.terminated?
    end
    Timecop.return
  end

  sub_test_case 'buffered output for broken chunks' do
    def flush_chunks
      @i.start
      @i.after_start

      @i.interrupt_flushes

      now = Time.parse('2016-04-13 18:33:30 -0700')
      Timecop.freeze(now)
      @i.emit_events("test.tag.1", dummy_event_stream())
      now = Time.parse('2016-04-13 18:33:32 -0700')
      Timecop.freeze(now)

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      waiting(4) { Thread.pass until @i.write_count > 0 }

      assert { @i.write_count > 0 }
      Timecop.freeze(now)
      @i.flush_thread_wakeup
    end

    def wait_flush(target_file)
      waiting(5) {
        target_dir = File.join(File.dirname(target_file), "*")
        while Dir.glob(target_dir).size.zero?
        end
      }
    end

    test 'backup chunk without secondary' do
      Fluent::SystemConfig.overwrite_system_config('root_dir' => TMP_DIR) do
        id = 'backup_test'
        hash = {
          'flush_interval' => 1,
          'flush_thread_burst_interval' => 0.1,
        }
        chunk_id = nil
        @i.configure(config_element('ROOT', '', {'@id' => id}, [config_element('buffer', 'tag', hash)]))
        @i.register(:write) { |chunk|
          chunk_id = chunk.unique_id;
          raise Fluent::UnrecoverableError, "yay, your #write must fail"
        }

        flush_chunks

        target = "#{TMP_DIR}/backup/worker0/#{id}/#{@i.dump_unique_id_hex(chunk_id)}.log"
        wait_flush(target)
        assert_true File.exist?(target)
        logs = @i.log.out.logs
        assert { logs.any? { |l| l.include?("got unrecoverable error in primary and no secondary") } }
      end
    end

    test 'backup chunk with same type secondary' do
      Fluent::SystemConfig.overwrite_system_config('root_dir' => TMP_DIR) do
        id = 'backup_test_with_same_secondary'
        hash = {
          'flush_interval' => 1,
          'flush_thread_burst_interval' => 0.1,
        }
        chunk_id = nil
        secconf = config_element('secondary','',{'@type' => 'backup_output'})
        @i.configure(config_element('ROOT', '', {'@id' => id}, [config_element('buffer', 'tag', hash), secconf]))
        @i.register(:write) { |chunk|
          chunk_id = chunk.unique_id;
          raise Fluent::UnrecoverableError, "yay, your #write must fail"
        }

        flush_chunks

        target = "#{TMP_DIR}/backup/worker0/#{id}/#{@i.dump_unique_id_hex(chunk_id)}.log"
        wait_flush(target)
        assert_true File.exist?(target)
        logs = @i.log.out.logs
        assert { logs.any? { |l| l.include?("got unrecoverable error in primary and secondary type is same as primary") } }
      end
    end

    test 'backup chunk with different type secondary' do
      Fluent::SystemConfig.overwrite_system_config('root_dir' => TMP_DIR) do
        id = 'backup_test_with_diff_secondary'
        hash = {
          'flush_interval' => 1,
          'flush_thread_burst_interval' => 0.1,
        }
        chunk_id = nil
        secconf = config_element('secondary','',{'@type' => 'backup_output2'})
        @i.configure(config_element('ROOT', '', {'@id' => id}, [config_element('buffer', 'tag', hash), secconf]))
        @i.register(:write) { |chunk|
          chunk_id = chunk.unique_id;
          raise Fluent::UnrecoverableError, "yay, your #write must fail"
        }
        @i.secondary.register(:write) { |chunk|
          raise Fluent::UnrecoverableError, "yay, your secondary #write must fail"
        }

        flush_chunks

        target = "#{TMP_DIR}/backup/worker0/#{id}/#{@i.dump_unique_id_hex(chunk_id)}.log"
        wait_flush(target)
        assert_true File.exist?(target)
        logs = @i.log.out.logs
        assert { logs.any? { |l| l.include?("got unrecoverable error in primary. Skip retry and flush chunk to secondary") } }
        assert { logs.any? { |l| l.include?("got an error in secondary for unrecoverable error") } }
      end
    end

    test 'backup chunk with async secondary' do
      Fluent::SystemConfig.overwrite_system_config('root_dir' => TMP_DIR) do
        id = 'backup_test_with_diff_secondary'
        hash = {
          'flush_interval' => 1,
          'flush_thread_burst_interval' => 0.1,
        }
        chunk_id = nil
        secconf = config_element('secondary','',{'@type' => 'backup_async_output'})
        @i.configure(config_element('ROOT', '', {'@id' => id}, [config_element('buffer', 'tag', hash), secconf]))
        @i.register(:write) { |chunk|
          chunk_id = chunk.unique_id;
          raise Fluent::UnrecoverableError, "yay, your #write must fail"
        }

        flush_chunks

        target = "#{TMP_DIR}/backup/worker0/#{id}/#{@i.dump_unique_id_hex(chunk_id)}.log"
        wait_flush(target)
        assert_true File.exist?(target)
        logs = @i.log.out.logs
        assert { logs.any? { |l| l.include?("got unrecoverable error in primary and secondary is async output") } }
      end
    end

    test 'chunk is thrown away when disable_chunk_backup is true' do
      Fluent::SystemConfig.overwrite_system_config('root_dir' => TMP_DIR) do
        id = 'backup_test'
        hash = {
          'flush_interval' => 1,
          'flush_thread_burst_interval' => 0.1,
          'disable_chunk_backup' => true
        }
        chunk_id = nil
        @i.configure(config_element('ROOT', '', {'@id' => id}, [config_element('buffer', 'tag', hash)]))
        @i.register(:write) { |chunk|
          chunk_id = chunk.unique_id;
          raise Fluent::UnrecoverableError, "yay, your #write must fail"
        }

        flush_chunks

        target = "#{TMP_DIR}/backup/worker0/#{id}/#{@i.dump_unique_id_hex(chunk_id)}.log"
        assert_false File.exist?(target)
        logs = @i.log.out.logs
        assert { logs.any? { |l| l.include?("disable_chunk_backup is true") } }
      end
    end
  end
end
