module Fluent
  TextParser.register_template('known_old', /^(?<message>.*)$/)
  Plugin.register_parser('known', /^(?<message>.*)$/)
end
