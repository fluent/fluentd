require 'time'
require 'fluent/parser'

class Fluent::ParserFilter < Fluent::Filter
  Fluent::Plugin.register_filter('parser', self)

  config_param :key_name, :string
  config_param :reserve_data, :bool, default: false
  config_param :inject_key_prefix, :string, default: nil
  config_param :replace_invalid_sequence, :bool, default: false
  config_param :hash_value_field, :string, default: nil
  config_param :suppress_parse_error_log, :bool, default: false
  config_param :time_parse, :bool, default: true
  config_param :ignore_key_not_exist, :bool, default: false
  config_param :emit_invalid_record_to_error, :bool, default: false

  attr_reader :parser

  def configure(conf)
    super

    @parser = Fluent::TextParser.new
    @parser.estimate_current_event = false
    @parser.configure(conf)
    if !@time_parse && @parser.parser.respond_to?("time_key=".to_sym)
      # disable parse time
      @parser.parser.time_key = nil
    end

    self
  end

  def filter_stream(tag, es)
    new_es = Fluent::MultiEventStream.new
    es.each do |time,record|
      raw_value = record[@key_name]
      if raw_value.nil?
        if @emit_invalid_record_to_error
          router.emit_error_event(tag, time, record, ArgumentError.new("#{@key_name} does not exist"))
        else
          log.warn "#{@key_name} does not exist" unless @ignore_key_not_exist
        end
        new_es.add(time, handle_parsed(tag, record, time, {})) if @reserve_data
        next
      end
      begin
        @parser.parse(raw_value) do |t,values|
          if values
            t ||= time
            r = handle_parsed(tag, record, t, values)
            new_es.add(t, r)
          else
            if @emit_invalid_record_to_error
              router.emit_error_event(tag, time, record, ::Fluent::ParserError.new("pattern not match with data '#{raw_value}'"))
            else
              log.warn "pattern not match with data '#{raw_value.dump}'" unless @suppress_parse_error_log
            end
            if @reserve_data
              t = time
              r = handle_parsed(tag, record, time, {})
              new_es.add(t, r)
            end
          end
        end
      rescue Fluent::ParserError => e
        if @emit_invalid_record_to_error
          router.emit_error_event(tag, time, record, e)
        else
          log.warn e.message unless @suppress_parse_error_log
        end
      rescue ArgumentError => e
        raise unless @replace_invalid_sequence
        raise unless e.message.index("invalid byte sequence in") == 0

        raw_value = replace_invalid_byte(raw_value)
        retry
      rescue => e
        if @emit_invalid_record_to_error
          router.emit_error_event(tag, time, record, Fluent::ParserError.new("parse failed #{e.message}"))
        else
          log.warn "parse failed #{e.message}" unless @suppress_parse_error_log
        end
      end
    end
    new_es
  end

  private

  def handle_parsed(tag, record, t, values)
    if values && @inject_key_prefix
      values = Hash[values.map{|k,v| [ @inject_key_prefix + k, v ]}]
    end
    r = @hash_value_field ? {@hash_value_field => values} : values
    if @reserve_data
      r = r ? record.merge(r) : record
    end
    r
  end

  def replace_invalid_byte(string)
    replace_options = { invalid: :replace, undef: :replace, replace: '?' }
    original_encoding = string.encoding
    temporal_encoding = (original_encoding == Encoding::UTF_8 ? Encoding::UTF_16BE : Encoding::UTF_8)
    string.encode(temporal_encoding, original_encoding, replace_options).encode(original_encoding)
  end
end
