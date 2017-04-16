module Fluent
  class TypecastFilter < Filter
    Fluent::Plugin.register_filter('typecast', self)

    include ::Fluent::TextParser::TypeConverter

    def filter(tag, time, record)
      filtered = record.map do |key, val|
        [key, convert_type(key, val)]
      end
      Hash[*filtered.flatten(1)]
    end
  end
end
