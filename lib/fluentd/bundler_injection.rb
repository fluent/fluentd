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

  module BundlerInjection
    class UIShell
      def say(msg, color, newline=nil)
        STDOUT.puts "#{color}#{msg}" unless msg.empty?
      end
    end

    def self.install(gemfile_path, opts={})
      return nil if ENV['BUNDLE_GEMFILE']
      ENV['BUNDLE_GEMFILE'] = gemfile_path

      require 'bundler'

      Bundler.ui = Bundler::UI::Shell.new(UIShell.new)
      #Bundler.ui.debug!
      Bundler.rubygems.ui = Bundler::UI::RGProxy.new(Bundler.ui)

      if install_path = opts[:install_path]
        Bundler.settings[:path] = install_path
        unless opts[:use_shared_gems]
          Bundler.settings[:disable_shared_gems] = '1'
        end
      end
      Bundler::Installer.install(Bundler.root, Bundler.definition, {})

      Bundler.setup
    end
  end

end
