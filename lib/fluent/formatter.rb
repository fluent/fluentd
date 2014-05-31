module Fluent
  require 'fluent/registry'

  # TextFormatter is module, not class. This is for reducing method call unlike TextParser.
  module TextFormatter
    module HandleTagAndTimeMixin
      def self.included(klass)
        klass.instance_eval {
          config_param :include_time_key, :bool, :default => false
          config_param :time_key, :string, :default => 'time'
          config_param :time_format, :string, :default => nil
          config_param :include_tag_key, :bool, :default => false
          config_param :tag_key, :string, :default => 'tag'
          config_param :localtime, :bool, :default => false
        }
      end

      def configure(conf)
        super

        @localtime = false if @utc
        @timef = TimeFormatter.new(@time_format, @localtime)
      end

      def filter_record(tag, time, record)
        if @include_tag_key
          record[@tag_key] = tag
        end
        if @include_time_key
          record[@time_key] = @timef.format(time)
        end
      end
    end

    class OutFileFormatter
      include Configurable
      include HandleTagAndTimeMixin

      config_param :output_tag_header, :bool, :default => true
      config_param :output_time_header, :bool, :default => true
      config_param :delimiter, :default => "\t" do |val|
        case val
        when /SPACE/i then ' '
        when /COMMA/i then ','
        else "\t"
        end
      end

      def configure(conf)
        super
      end

      def format(tag, time, record)
        filter_record(tag, time, record)
        header = ''
        header << "#{@timef.format(time)}#{@field_separator}" if @output_time_header
        header << "#{tag}#{@field_separator}" if @output_tag_header
        "#{header}#{Yajl.dump(record)}\n"
      end
    end

    class JSONFormatter
      include Configurable
      include HandleTagAndTimeMixin

      # Other formatter also should have this paramter?
      config_param :keep_time_as_number, :bool, :default => false

      def configure(conf)
        super

        if @keep_time_as_number
          @include_time_key = false
        end
      end

      def format(tag, time, record)
        filter_record(tag, time, record)
        record[@time_key] = time if @keep_time_as_number
        "#{Yajl.dump(record)}\n"
      end
    end

    # Should use 'ltsv' gem?
    class LabeledTSVFormatter
      include Configurable
      include HandleTagAndTimeMixin

      config_param :delimiter, :string, :default => "\t"
      config_param :label_delimiter, :string, :default =>  ":"

      def format(tag, time, record)
        filter_record(tag, time, record)
        record.inject('') { |result, pair|
          result << @delimiter if result.length.nonzero?
          result << "#{pair.first}#{@label_delimiter}#{pair.last}"
        }
      end
    end

    # More better name?
    class OneKeyFormatter
      include Configurable

      config_param :message_key, :string, :default => 'message'

      def format(tag, time, record)
        record[@message_key]
      end
    end

    TEMPLATE_REGISTRY = Registry.new(:formatter_type, 'fluent/plugin/formatter_')
    {
      'out_file' => Proc.new { OutFileFormatter.new },
      'json' => Proc.new { JSONFormatter.new },
      'ltsv' => Proc.new { LabeledTSVFormatter.new },
      'onekey' => Proc.new { OneKeyFormatter.new },
    }.each { |name, factory|
      TEMPLATE_REGISTRY.register(name, factory)
    }

    def self.register_template(name, factory)
      TEMPLATE_REGISTRY.register(name, factory)
    end

    def self.create(conf)
      format = conf['format']

      if format.nil?
        if required
          raise ConfigError, "'format' parameter is required"
        else
          return nil
        end
      end

      # built-in template
      begin
        factory = TEMPLATE_REGISTRY.lookup(format)
      rescue ConfigError => e
        raise ConfigError, "unknown format: '#{format}'"
      end

      formatter = factory.call
      formatter.configure(conf)
      formatter
    end
  end
end
