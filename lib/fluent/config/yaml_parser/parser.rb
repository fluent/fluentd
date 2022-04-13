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

require 'fluent/config/yaml_parser/section_builder'

module Fluent
  module Config
    module YamlParser
      class Parser
        def initialize(config, indent: 2)
          @base_indent = indent
          @config = config
        end

        def build
          s = @config['system'] && system_config_build(@config['system'])
          c = @config['config'] && config_build(@config['config'], root: true)
          RootBuilder.new(s, c)
        end

        private

        def system_config_build(config)
          section_build('system', config)
        end

        def config_build(config, indent: 0, root: false)
          sb = SectionBodyBuilder.new(indent, root: root)
          config.each do |c|
            if (lc = c.delete('label'))
              sb.add_section(label_build(lc, indent: indent))
            end

            if (sc = c.delete('source'))
              sb.add_section(source_build(sc, indent: indent))
            end

            if (fc = c.delete('filter'))
              sb.add_section(filter_build(fc, indent: indent))
            end

            if (mc = c.delete('match'))
              sb.add_section(match_build(mc, indent: indent))
            end

            if (wc = c.delete('worker'))
              sb.add_section(worker_build(wc, indent: indent))
            end

            included_sections_build(c, sb, indent: indent)
          end

          sb
        end

        def label_build(config, indent: 0)
          config = config.dup
          name = config.delete('$name')
          c = config.delete('config')
          SectionBuilder.new('label', config_build(c, indent: indent + @base_indent), indent, name)
        end

        def worker_build(config, indent: 0)
          config = config.dup
          num = config.delete('$arg')
          c = config.delete('config')
          SectionBuilder.new('worker', config_build(c, indent: indent + @base_indent), indent, num)
        end

        def source_build(config, indent: 0)
          section_build('source', config, indent: indent)
        end

        def filter_build(config, indent: 0)
          config = config.dup
          tag = config.delete('$tag')
          if tag.is_a?(Array)
            section_build('filter', config, indent: indent, arg: tag&.join(','))
          else
            section_build('filter', config, indent: indent, arg: tag)
          end
        end

        def match_build(config, indent: 0)
          config = config.dup
          tag = config.delete('$tag')
          if tag.is_a?(Array)
            section_build('match', config, indent: indent, arg: tag&.join(','))
          else
            section_build('match', config, indent: indent, arg: tag)
          end
        end

        def included_sections_build(config, section_builder, indent: 0)
          config.each_entry do |e|
            k = e.keys.first
            cc = e.delete(k)
            case k
            when 'label'
              section_builder.add_section(label_build(cc, indent: indent))
            when 'worker'
              section_builder.add_section(worker_build(cc, indent: indent))
            when 'source'
              section_builder.add_section(source_build(cc, indent: indent))
            when 'filter'
              section_builder.add_section(filter_build(cc, indent: indent))
            when 'match'
              section_builder.add_section(match_build(cc, indent: indent))
            end
          end
        end

        def section_build(name, config, indent: 0, arg: nil)
          sb = SectionBodyBuilder.new(indent + @base_indent)

          if (v = config.delete('$type'))
            sb.add_line('@type', v)
          end

          if (v = config.delete('$label'))
            sb.add_line('@label', v)
          end

          if (v = config.delete('$id'))
            sb.add_line('@id', v)
          end

          config.each do |key, val|
            if val.is_a?(Array)
              val.each do |v|
                sb.add_section(section_build(key, v, indent: indent + @base_indent))
              end
            elsif val.is_a?(Hash)
              harg = val.delete('$arg')
              if harg.is_a?(Array)
                # To prevent to generate invalid configuration for arg.
                # "arg" should be String object and concatenated by ","
                # when two or more objects are specified there.
                sb.add_section(section_build(key, val, indent: indent + @base_indent, arg: harg&.join(',')))
              else
                sb.add_section(section_build(key, val, indent: indent + @base_indent, arg: harg))
              end
            else
              sb.add_line(key, val)
            end
          end

          SectionBuilder.new(name, sb, indent, arg)
        end
      end
    end
  end
end
