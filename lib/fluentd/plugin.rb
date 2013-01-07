#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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

  class PluginRegistry
    def initialize(kind, search_prefix)
      @kind = kind
      @search_prefix = search_prefix
      @map = {}
    end

    attr_reader :kind

    def register(type, klass)
      type = type.to_sym
      #@log.trace { "registered #{@kind} plugin '#{type}'" }
      @map[type] = klass
    end

    def lookup(type)
      type = type.to_sym
      if klass = @map[type]
        return klass
      end
      search(type)
      if klass = @map[type]
        return klass
      end
      raise ConfigError, "Unknown #{@kind} plugin '#{type}'. Run 'gem search -rd fluent-plugin' to find plugins"
    end

    def search(type)
      path = "#{@search_prefix}#{type}"

      # prefer LOAD_PATH than gems
      files = $LOAD_PATH.map {|lp|
        lpath = File.join(lp, "#{path}.rb")
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

  class PluginClass
    def self.load!(conf)
      ## TODO log?
      #if Fluentd.const_defined?(:Plugin)
      #  Fluentd.module_eval { remove_const(:Plugin) }
      #end

      plugin = Fluentd.const_set(:Plugin, PluginClass.new)
      begin
        plugin.load_plugins
      ensure
        Fluentd.module_eval { remove_const(:Plugin) }
      end

      plugin
    end

    def initialize
      @input = PluginRegistry.new(:input, 'fluent/plugin/in_')
      @output = PluginRegistry.new(:output, 'fluent/plugin/out_')
      @buffer = PluginRegistry.new(:buffer, 'fluent/plugin/buf_')
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

    def new_input(type)
      @input.lookup(type).new
    end

    def new_output(type)
      @output.lookup(type).new
    end

    def new_buffer(type)
      @buffer.lookup(type).new
    end

    def load_plugins
      dir = File.join(File.dirname(__FILE__), 'plugins')
      load_plugin_dir(dir)
      load_gem_plugins
    end

    def load_plugin_dir(dir)
      dir = File.expand_path(dir)
      Dir.entries(dir).sort.each {|fname|
        if fname =~ /\.rb$/
          require File.join(dir, fname)
        end
      }
      nil
    end

    private
    def load_gem_plugins
      return unless defined? Gem
      plugins = Gem.find_files('fluent_plugin')

      plugins.each {|plugin|
        begin
          load plugin
        rescue ::Exception => e
          msg = "#{plugin.inspect}: #{e.message} (#{e.class})"
          $log.warn "Error loading Fluent plugin #{msg}"
        end
      }
    end
  end

end
