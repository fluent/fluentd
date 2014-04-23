module Fluent
  require 'fluent/config/error'

  class Registry
    def initialize(kind, search_prefix)
      @kind = kind
      @search_prefix = search_prefix
      @map = {}
    end

    attr_reader :kind

    def register(type, value)
      type = type.to_sym
      @map[type] = value
    end

    def lookup(type)
      type = type.to_sym
      if value = @map[type]
        return value
      end
      search(type)
      if value = @map[type]
        return value
      end
      raise ConfigError, "Unknown #{@kind} plugin '#{type}'. Run 'gem search -rd fluentd-plugin' to find plugins"  # TODO error class
    end

    def search(type)
      path = "#{@search_prefix}#{type}"

      # prefer LOAD_PATH than gems
      files = $LOAD_PATH.map { |lp|
        lpath = File.expand_path(File.join(lp, "#{path}.rb"))
        File.exist?(lpath) ? lpath : nil
      }.compact
      unless files.empty?
        # prefer newer version
        require files.sort.last
        return
      end

      # search gems
      if defined?(::Gem::Specification) && ::Gem::Specification.respond_to?(:find_all)
        specs = Gem::Specification.find_all { |spec|
          spec.contains_requirable_file? path
        }

        # prefer newer version
        specs = specs.sort_by { |spec| spec.version }
        if spec = specs.last
          spec.require_paths.each { |lib|
            file = "#{spec.full_gem_path}/#{lib}/#{path}"
            require file
          }
        end

        # backward compatibility for rubygems < 1.8
      elsif defined?(::Gem) && ::Gem.respond_to?(:searcher)
        #files = Gem.find_files(path).sort
        specs = Gem.searcher.find_all(path)

        # prefer newer version
        specs = specs.sort_by { |spec| spec.version }
        specs.reverse_each { |spec|
          files = Gem.searcher.matching_files(spec, path)
          unless files.empty?
            require files.first
            break
          end
        }
      end
    end
  end
end
