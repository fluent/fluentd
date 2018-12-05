module Fluent
  Plugin.register_formatter(
    'test2',
    Proc.new { |tag, time, record|
      "#{tag}:#{time}:#{record.size}"
    })
end
