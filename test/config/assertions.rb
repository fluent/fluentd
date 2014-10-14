require 'test/unit/assertions'

module Test::Unit::Assertions
  def assert_text_parsed_as(expected, actual)
    msg = parse_text(actual).inspect rescue 'failed'
    msg = "expected that #{actual.inspect} would be a parsed as #{expected.inspect} but got #{msg}"
    assert_block(msg) {
      v = parse_text(actual)
      if expected.is_a?(Float)
        v.is_a?(Float) && (v == obj || (v.nan? && obj.nan?) || (v - obj).abs < 0.000001)
      else
        v == expected
      end
    }
  end

  def assert_text_parsed_as_json(expected, actual)
    msg = parse_text(actual).inspect rescue 'failed'
    msg = "expected that #{actual.inspect} would be a parsed as #{expected.inspect} but got #{msg}"
    assert_block(msg) {
      v = JSON.parse(parse_text(actual))
      v == expected
    }
  end

  def assert_parse_error(actual)
    msg = begin
            parse_text(actual).inspect
          rescue => e
            e.inspect
          end
    msg = "expected that #{actual.inspect} would cause a parse error but got #{msg}"
    assert_block(msg) {
      begin
        parse_text(actual)
        false
      rescue Fluent::ConfigParseError
        true
      end
    }
  end
end
