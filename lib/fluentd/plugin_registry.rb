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
module Fluentd

  require 'fluentd/config_error'

  #
  # PluginRegistry searches & loads plugins and manages
  # instantiation of the plugins.
  #
  class PluginRegistry
    def initialize
      @input = Registry.new(:input, 'fluentd/plugin/in_')
      @output = Registry.new(:output, 'fluentd/plugin/out_')
      @buffer = Registry.new(:buffer, 'fluentd/plugin/buf_')
    end

    def register_input(type, klass)
      @input.register(type, klass)
    end

    def register_output(type, klass)
      @output.register(type, klass)
    end

    def register_buffer(type, klass)
      @buffer.register(type, klass)
    end

    def new_input(parent_agent, type)
      a = @input.lookup(type).new
      a.parent_agent = parent_agent
      return a
    end

    def new_output(parent_agent, type)
      a = @output.lookup(type).new
      a.parent_agent = parent_agent
      return a
    end

    def new_buffer(type)
      @buffer.lookup(type).new
    end

    # called by Worker
    def self.load_built_in_plugins
      dir = File.join(File.dirname(__FILE__), 'plugin')
      load_plugin_dir(dir)
      load_gem_plugins
    end

    def self.load_plugin_dir(dir)
      dir = File.expand_path(dir)
      Dir.entries(dir).sort.each {|fname|
        if fname =~ /(?:in|out)_.*\.rb$/
          require File.join(dir, fname)
        end
      }
      nil
    end

    def self.load_gem_plugins
      return unless defined? Gem
      plugins = Gem.find_files('fluentd_plugin')

      plugins.each {|plugin|
        begin
          load plugin
        rescue ::Exception => e
          msg = "#{plugin.inspect}: #{e.message} (#{e.class})"
          Engine.log.warn "Error loading Fluent plugin #{msg}"  # TODO log
        end
      }
    end
    private_class_method :load_gem_plugins

    class Registry
      def initialize(kind, search_prefix)
        @kind = kind
        @search_prefix = search_prefix
        @map = {}
      end

      attr_reader :kind

      def register(type, value)
        type = type.to_sym
        #Engine.log.trace { "registered #{@kind} plugin '#{type}'" }
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
        files = $LOAD_PATH.map {|lp|
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
          specs = Gem::Specification.find_all {|spec|
            spec.contains_requirable_file? path
          }

          # prefer newer version
          specs = specs.sort_by {|spec| spec.version }
          if spec = specs.last
            spec.require_paths.each {|lib|
              file = "#{spec.full_gem_path}/#{lib}/#{path}"
              require file
            }
          end

          # backward compatibility for rubygems < 1.8
        elsif defined?(::Gem) && ::Gem.respond_to?(:searcher)
          #files = Gem.find_files(path).sort
          specs = Gem.searcher.find_all(path)

          # prefer newer version
          specs = specs.sort_by {|spec| spec.version }
          specs.reverse_each {|spec|
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

end
