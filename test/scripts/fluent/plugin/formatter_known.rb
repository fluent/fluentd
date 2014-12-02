module Fluent
  TextFormatter.register_template('known_old', Proc.new { |tag, time, record|
      "#{tag}:#{time}:#{record.size}"
    })
  Plugin.register_formatter('known', Proc.new { |tag, time, record|
      "#{tag}:#{time}:#{record.size}"
    })
end
