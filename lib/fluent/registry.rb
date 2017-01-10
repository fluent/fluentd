#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'fluent/config/error'

module Fluent
  class Registry
    DEFAULT_PLUGIN_PATH = File.expand_path('../plugin', __FILE__)
    FLUENT_LIB_PATH = File.dirname(File.dirname(DEFAULT_PLUGIN_PATH))

    def initialize(kind, search_prefix, dir_search_prefix: nil)
      @kind = kind
      @search_prefix = search_prefix
      @dir_search_prefix = dir_search_prefix
      @map = {}
      @paths = []
    end

    attr_reader :kind, :paths, :map, :dir_search_prefix

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
      raise ConfigError, "Unknown #{@kind} plugin '#{type}'. Run 'gem search -rd fluent-plugin' to find plugins"  # TODO error class
    end

    def reverse_lookup(value)
      @map.each do |k, v|
        return k if v == value
      end
      nil
    end

    def search(type)
      # search from additional plugin directories
      if @dir_search_prefix
        path = "#{@dir_search_prefix}#{type}"
        files = @paths.map { |lp|
          lpath = File.expand_path(File.join(lp, "#{path}.rb"))
          File.exist?(lpath) ? lpath : nil
        }.compact
        unless files.empty?
          # prefer newer version
          require files.sort.last
          return
        end
      end

      path = "#{@search_prefix}#{type}"

      # prefer LOAD_PATH than gems
      files = $LOAD_PATH.map { |lp|
        if lp == FLUENT_LIB_PATH
          nil
        else
          lpath = File.expand_path(File.join(lp, "#{path}.rb"))
          File.exist?(lpath) ? lpath : nil
        end
      }.compact
      unless files.empty?
        # prefer newer version
        require files.sort.last
        return
      end

      # Find from gems and prefer newer version
      specs = Gem::Specification.find_all { |spec|
        if spec.name == 'fluentd'.freeze
          false
        else
          spec.contains_requirable_file? path
        end
      }.sort_by { |spec| spec.version }
      if spec = specs.last
        spec.require_paths.each { |lib|
          file = "#{spec.full_gem_path}/#{lib}/#{path}"
          if File.exist?("#{file}.rb")
            require file
            return
          end
        }
      end

      # Lastly, load built-in plugin
      lpath = File.expand_path(File.join(FLUENT_LIB_PATH, "#{@search_prefix}#{type}.rb"))
      if File.exist?(lpath)
        require lpath
        return
      end
    end
  end
end
