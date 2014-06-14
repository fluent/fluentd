shared_context 'config_helper' do
  RSpec::Matchers.define :be_parsed_as do |obj|
    match do |text|
      v = parse_text(text)
      if obj.is_a?(Float)
        v.is_a?(Float) && (v == obj || (v.nan? && obj.nan?) || (v - obj).abs < 0.000001)
      else
        v == obj
      end
    end

    failure_message do |text|
      msg = parse_text(text).inspect rescue 'failed'
      "expected that #{text.inspect} would be a parsed as #{obj.inspect} but got #{msg}"
    end
  end

  RSpec::Matchers.define :be_parsed_as_json do |obj|
    match do |text|
      v = JSON.parse(parse_text(text))
      v == obj
    end

    failure_message do |text|
      msg = parse_text(text).inspect rescue 'failed'
      "expected that #{text.inspect} would be a parsed as #{obj.inspect} but got #{msg}"
    end
  end

  RSpec::Matchers.define :be_parse_error do |obj|
    match do |text|
      begin
        parse_text(text)
        false
      rescue Fluent::ConfigParseError
        true
      end
    end

    failure_message do |text|
      begin
        msg = parse_text(text).inspect
      rescue
        msg = $!.inspect
      end
      "expected that #{text.inspect} would cause a parse error but got #{msg}"
    end
  end
end

