module Fluent
  Plugin.register_formatter(
    'test1',
    Proc.new { |tag, time, record|
      "#{tag}:#{time}:#{record.size}"
    })
end
