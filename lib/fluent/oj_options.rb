require 'fluent/config'

module Fluent
  class OjOptions
    OPTIONS = {
      'bigdecimal_load': :symbol,
      'max_nesting': :integer,
      'mode': :symbol,
      'use_to_json': :bool
    }

    ALLOWED_VALUES = {
      'bigdecimal_load': %i[bigdecimal float auto],
      'mode': %i[strict null compat json rails object custom]
    }

    DEFAULTS = {
      'bigdecimal_load': :float,
      'mode': :compat,
      'use_to_json': true
    }

    def self.get_options
      options = {}
      DEFAULTS.each { |key, value| options[key] = value }

      OPTIONS.each do |key, type|
        env_value = ENV["FLUENT_OJ_OPTION_#{key.upcase}"]
        next if env_value.nil?

        cast_value = Fluent::Config.reformatted_value(OPTIONS[key], env_value, { strict: true })
        next if cast_value.nil?

        next if ALLOWED_VALUES[key] && !ALLOWED_VALUES[key].include?(cast_value)

        options[key.to_sym] = cast_value
      end

      options
    end
  end
end
