require 'fluent/registry'

module Fluent
  module Validator
    class PluginName

      PLUGIN_PREFIX = ["in_", "out_", "buf_", "filter_", "formatter_", "parser_", "sd_", "storage_"]

      def initialize(paths, dir_search_prefix, search_prefix)
        @paths = paths
        @dir_search_prefix = dir_search_prefix
        @search_prefix = search_prefix
        @plugin_path = nil
      end

      def guess_plugin_name(type, kind = nil)
        prefix_pattern = PLUGIN_PREFIX.find { |prefix| type.to_s.start_with?(prefix) }
        plugin_name = if prefix_pattern
                        type.to_s.sub(/\A#{prefix_pattern}/, '')
                      else
                        type.to_s
                      end
        plugin_name
      end

      # Plugin must be searched from: (See fluent/registry.rb)
      #  @paths > LOAD_PATH > gems > built-in plugin
      def valid?(type)
        files = @paths.collect do |plugin_dir|
          plugin_name = guess_plugin_name(type)
          prefixed_plugin_path = File.expand_path(File.join(plugin_dir, "#{@dir_search_prefix}#{plugin_name}.rb"))
          plugin_path = File.expand_path(File.join(plugin_dir, "#{type}.rb"))
          if File.exist?(prefixed_plugin_path)
            File.basename(prefixed_plugin_path, ".rb") == type.to_s ? prefixed_plugin_path : nil
          elsif File.exist?(plugin_path)
            File.basename(plugin_path, ".rb") == type.to_s ? plugin_path : nil
          else
            nil
          end
        end.compact
        unless files.empty?
          @plugin_path = files.sort.last
          return false
        end
        
        $LOAD_PATH.each do |load_path|
          next if load_path == Fluent::Registry::FLUENT_LIB_PATH # skip build-in
          plugin_path = File.expand_path(File.join(load_path, "#{type}.rb"))
          if File.exist?(plugin_path)
            if File.basename(plugin_path, ".rb") == type.to_s
              @plugin_path = plugin_path
              return false
            end
          end
        end

        path = "#{@search_prefix}#{type}"
        specs = Gem::Specification.find_all do |spec|
          if spec.name == 'fluentd'.freeze
            false
          else
            spec.contains_requirable_file?(path)
          end
        end.sort_by { |spec| spec.version }
        if spec = specs.last
          spec.require_paths.each do |lib|
            file = "#{spec.full_gem_path}/#{lib}/#{path}"
            if File.exist?("#{file}.rb")
              if path == type.to_s
                @plugin_path = "#{file}.rb"
                return false
              end
            end
          end
        end

        # skip checking built-in plugin
        true
      end
    end
  end
end
