module Fluent
  TextFormatter.register_template('known', Proc.new { |tag, time, record|
      "#{tag}:#{time}:#{record.size}"
    })
end
