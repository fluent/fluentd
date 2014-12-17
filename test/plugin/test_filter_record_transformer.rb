require_relative '../helper'
require 'timecop'
require 'fluent/plugin/filter_record_transformer'

class RecordTransformerFilterTest < Test::Unit::TestCase
  include Fluent

  setup do
    Test.setup
    @hostname = Socket.gethostname.chomp
    @tag = 'test.tag'
    @tag_parts = @tag.split('.')
    @time = Time.utc(1,2,3,4,5,2010,nil,nil,nil,nil)
    Timecop.freeze(@time)
  end

  teardown do
    Timecop.return
  end

  def create_driver(conf = '')
    Test::FilterTestDriver.new(RecordTransformerFilter, @tag).configure(conf, true)
  end

  sub_test_case 'configure' do
    test 'check default' do
      assert_nothing_raised do
        create_driver
      end
    end

    test "keep_keys must be specified together with renew_record true" do
      assert_raise(Fluent::ConfigError) do
        create_driver(%[keep_keys a])
      end
    end
  end

  sub_test_case "test options" do
    def emit(config, msgs = [''])
      d = create_driver(config)
      d.run {
        msgs.each { |msg|
          d.emit({'foo' => 'bar', 'message' => msg}, @time)
        }
      }.filtered
    end

    CONFIG = %[
      <record>
        hostname ${hostname}
        tag ${tag}
        time ${time}
        message ${hostname} ${tag_parts[-1]} ${message}
      </record>
    ]

    test 'typical usage' do
      msgs = ['1', '2']
      es = emit(CONFIG, msgs)
      es.each_with_index do |(t, r), i|
        assert_equal('bar', r['foo'])
        assert_equal(@hostname, r['hostname'])
        assert_equal(@tag, r['tag'])
        assert_equal(@time.to_s, r['time'])
        assert_equal("#{@hostname} #{@tag_parts[-1]} #{msgs[i]}", r['message'])
      end
    end

    test 'remove_keys' do
      config = CONFIG + %[remove_keys foo,message]
      es = emit(config)
      es.each_with_index do |(t, r), i|
        assert_not_include(r, 'foo')
        assert_equal(@hostname, r['hostname'])
        assert_equal(@tag, r['tag'])
        assert_equal(@time.to_s, r['time'])
        assert_not_include(r, 'message')
      end
    end

    test 'renew_record' do
      config = CONFIG + %[renew_record true]
      msgs = ['1', '2']
      es = emit(config, msgs)
      es.each_with_index do |(t, r), i|
        assert_not_include(r, 'foo')
        assert_equal(@hostname, r['hostname'])
        assert_equal(@tag, r['tag'])
        assert_equal(@time.to_s, r['time'])
        assert_equal("#{@hostname} #{@tag_parts[-1]} #{msgs[i]}", r['message'])
      end
    end

    test 'keep_keys' do
      config = %[renew_record true\nkeep_keys foo,message]
      msgs = ['1', '2']
      es = emit(config, msgs)
      es.each_with_index do |(t, r), i|
        assert_equal('bar', r['foo'])
        assert_equal(msgs[i], r['message'])
      end
    end

    test 'enable_ruby' do
      config = %[
        enable_ruby yes
        <record>
          message ${hostname} ${tag_parts.last} ${URI.encode(message)}
        </record>
      ]
      msgs = ['1', '2']
      es = emit(config, msgs)
      es.each_with_index do |(t, r), i|
        assert_equal("#{@hostname} #{@tag_parts[-1]} #{msgs[i]}", r['message'])
      end
    end
  end

  sub_test_case 'test placeholders' do
    def emit(config, msgs = [''])
      d = create_driver(config)
      yield d if block_given?
      d.run {
        msgs.each do |msg|
          d.emit({'eventType0' => 'bar', 'message' => msg}, @time)
        end
      }.filtered
    end

    %w[yes no].each do |enable_ruby|
      test "hostname with enble_ruby #{enable_ruby}" do
        config = %[
          enable_ruby #{enable_ruby}
          <record>
            message ${hostname}
          </record>
        ]
        es = emit(config)
        es.each do |t, r|
          assert_equal(@hostname, r['message'])
        end
      end

      test "tag with enable_ruby #{enable_ruby}" do
        config = %[
          enable_ruby #{enable_ruby}
          <record>
            message ${tag}
          </record>
        ]
        es = emit(config)
        es.each do |t, r|
          assert_equal(@tag, r['message'])
        end
      end

      test "tag_parts with enable_ruby #{enable_ruby}" do
        config = %[
          enable_ruby #{enable_ruby}
          <record>
            message ${tag_parts[0]} ${tag_parts[-1]}
          </record>
        ]
        expected = "#{@tag.split('.').first} #{@tag.split('.').last}"
        es = emit(config)
        es.each do |t, r|
          assert_equal(expected, r['message'])
        end
      end

      test "${tag_prefix[N]} and ${tag_suffix[N]} with enable_ruby #{enable_ruby}" do
        config = %[
          enable_ruby #{enable_ruby}
          <record>
            message ${tag_prefix[1]} ${tag_prefix[-2]} ${tag_suffix[2]} ${tag_suffix[-3]}
          </record>
        ]
        @tag = 'prefix.test.tag.suffix'
        expected = "prefix.test prefix.test.tag tag.suffix test.tag.suffix"
        es = emit(config)
        es.each do |t, r|
          assert_equal(expected, r['message'])
        end
      end

      test "time with enable_ruby #{enable_ruby}" do
        config = %[
          enable_ruby #{enable_ruby}
          <record>
            message ${time}
          </record>
        ]
        es = emit(config)
        es.each do |t, r|
          assert_equal(@time.to_s, r['message'])
        end
      end

      test "record keys with enable_ruby #{enable_ruby}" do
        config = %[
          enable_ruby #{enable_ruby}
          remove_keys eventType0
          <record>
            message bar ${message}
            eventtype ${eventType0}
          </record>
        ]
        msgs = ['1', '2']
        es = emit(config, msgs)
        es.each_with_index do |(t, r), i|
          assert_not_include(r, 'eventType0')
          assert_equal("bar", r['eventtype'])
          assert_equal("bar #{msgs[i]}", r['message'])
        end
      end
    end

    test 'unknown placeholder (enable_ruby no)' do
      config = %[
        enable_ruby no
        <record>
          message ${unknown}
        </record>
      ]
      emit(config) { |d|
        mock(d.instance.log).warn("unknown placeholder `${unknown}` found")
      }
    end

    test 'failed to expand (enable_ruby yes)' do
      config = %[
        enable_ruby yes
        <record>
          message ${unknown['bar']}
        </record>
      ]
      es = emit(config) { |d|
        mock(d.instance.log).warn("failed to expand `${unknown['bar']}`", anything)
      }
      es.each do |t, r|
        assert_nil(r['message'])
      end
    end
  end
end
