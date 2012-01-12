#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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
module Fluent


class PluginClass
  def initialize
    @input = {}
    @output = {}
    @buffer = {}
  end

  def register_input(type, klass)
    register_impl('input', @input, type, klass)
  end

  def register_output(type, klass)
    register_impl('output', @output, type, klass)
  end

  def register_buffer(type, klass)
    register_impl('buffer', @buffer, type, klass)
  end

  def new_input(type)
    new_impl('input', @input, type)
  end

  def new_output(type)
    new_impl('output', @output, type)
  end

  def new_buffer(type)
    new_impl('buffer', @buffer, type)
  end

  def load_plugins
    dir = File.join(File.dirname(__FILE__), "plugin")
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

  def register_impl(name, map, type, klass)
    map[type] = klass
    $log.trace { "registered #{name} plugin '#{type}'" }
    nil
  end

  def new_impl(name, map, type)
    if klass = map[type]
      return klass.new
    end
    try_load_plugin(name, type)
    if klass = map[type]
      return klass.new
    end
    raise ConfigError, "Unknown #{name} plugin '#{type}'. Run 'gem search -rd fluent-plugin' to find plugins"
  end

  def try_load_plugin(name, type)
    case name
    when 'input'
      path = "fluent/plugin/in_#{type}"
    when 'output'
      path = "fluent/plugin/out_#{type}"
    when 'buffer'
      path = "fluent/plugin/buf_#{type}"
    else
      return
    end

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

Plugin = PluginClass.new


end

