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

require 'fluent/plugin/parser'

require 'fluent/time'

module Fluent
  module Plugin
    class SyslogParser < Parser
      Plugin.register_parser('syslog', self)

      # TODO: Remove them since these regexps are no longer needed. but keep them for compatibility for now
      # From existence TextParser pattern
      REGEXP = /^(?<time>[^ ]*\s*[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[^ :\[]*)(?:\[(?<pid>[0-9]+)\])?(?:[^\:]*\:)? *(?<message>.*)$/
      # From in_syslog default pattern
      REGEXP_WITH_PRI = /^\<(?<pri>[0-9]+)\>(?<time>[^ ]* {1,2}[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[^ :\[]*)(?:\[(?<pid>[0-9]+)\])?(?:[^\:]*\:)? *(?<message>.*)$/
      REGEXP_RFC5424 = <<~'EOS'.chomp
        (?<time>[^ ]+) (?<host>[!-~]{1,255}) (?<ident>[!-~]{1,48}) (?<pid>[!-~]{1,128}) (?<msgid>[!-~]{1,32}) (?<extradata>(?:\-|(?:\[.*?(?<!\\)\])+))(?: (?<message>.+))?
      EOS
      REGEXP_RFC5424_NO_PRI = Regexp.new(<<~'EOS'.chomp % REGEXP_RFC5424, Regexp::MULTILINE)
        \A%s\z
      EOS
      REGEXP_RFC5424_WITH_PRI = Regexp.new(<<~'EOS'.chomp % REGEXP_RFC5424, Regexp::MULTILINE)
        \A<(?<pri>[0-9]{1,3})\>[1-9]\d{0,2} %s\z
      EOS

      REGEXP_DETECT_RFC5424 = /^\<[0-9]{1,3}\>[1-9]\d{0,2}/

      RFC3164_WITHOUT_TIME_AND_PRI_REGEXP = /(?<host>[^ ]*) (?<ident>[^ :\[]*)(?:\[(?<pid>[0-9]+)\])?(?:[^\:]*\:)? *(?<message>.*)$/
      RFC3164_PRI_REGEXP = /^<(?<pri>[0-9]{1,3})>/

      RFC5424_WITHOUT_TIME_AND_PRI_REGEXP = /(?<host>[!-~]{1,255}) (?<ident>[!-~]{1,48}) (?<pid>[!-~]{1,128}) (?<msgid>[!-~]{1,32}) (?<extradata>(?:\-|(?:\[.*?(?<!\\)\])+))(?: (?<message>.+))?\z/m
      RFC5424_PRI_REGEXP = /^<(?<pri>\d{1,3})>(?<version>\d\d{0,2})\s/

      config_set_default :time_format, "%b %d %H:%M:%S"
      desc 'If the incoming logs have priority prefix, e.g. <9>, set true'
      config_param :with_priority, :bool, default: false
      desc 'Specify protocol format'
      config_param :message_format, :enum, list: [:rfc3164, :rfc5424, :auto], default: :rfc3164
      desc 'Specify time format for event time for rfc5424 protocol'
      config_param :rfc5424_time_format, :string, default: "%Y-%m-%dT%H:%M:%S.%L%z"
      desc 'The parser type used to parse syslog message'
      config_param :parser_type, :enum, list: [:regexp, :string], default: :regexp
      desc 'support colonless ident in string parser'
      config_param :support_colonless_ident, :bool, default: true

      def initialize
        super
        @mutex = Mutex.new
        @space_count = nil
        @space_count_rfc5424 = nil
      end

      def configure(conf)
        super

        @time_parser_rfc3164 = @time_parser_rfc5424 = nil
        @time_parser_rfc5424_without_subseconds = nil
        @support_rfc5424_without_subseconds = false
        @regexp_parser = @parser_type == :regexp
        @regexp = case @message_format
                  when :rfc3164
                    if @regexp_parser
                      class << self
                        alias_method :parse, :parse_rfc3164_regex
                      end
                    else
                      class << self
                        alias_method :parse, :parse_rfc3164
                      end
                    end
                    RFC3164_WITHOUT_TIME_AND_PRI_REGEXP
                  when :rfc5424
                    class << self
                      alias_method :parse, :parse_rfc5424_regex
                    end
                    @time_format = @rfc5424_time_format unless conf.has_key?('time_format')
                    @support_rfc5424_without_subseconds = true
                    RFC5424_WITHOUT_TIME_AND_PRI_REGEXP
                  when :auto
                    class << self
                      alias_method :parse, :parse_auto
                    end
                    @time_parser_rfc3164 = time_parser_create(format: @time_format)
                    @time_parser_rfc5424 = time_parser_create(format: @rfc5424_time_format)
                    nil
                  end

        @space_count = @time_format.squeeze(' ').count(' ') + 1
        @space_count_rfc5424 = @rfc5424_time_format.squeeze(' ').count(' ') + 1
        @time_parser = time_parser_create
        @time_parser_rfc5424_without_subseconds = time_parser_create(format: "%Y-%m-%dT%H:%M:%S%z")
      end

      # this method is for tests
      def patterns
        {'format' => @regexp, 'time_format' => @time_format}
      end

      def parse(text)
        # This is overwritten in configure
      end

      def parse_auto(text, &block)
        if REGEXP_DETECT_RFC5424.match(text)
          @regexp = RFC5424_WITHOUT_TIME_AND_PRI_REGEXP
          @time_parser = @time_parser_rfc5424
          @support_rfc5424_without_subseconds = true
          parse_rfc5424_regex(text, &block)
        else
          @regexp = RFC3164_WITHOUT_TIME_AND_PRI_REGEXP
          @time_parser = @time_parser_rfc3164
          if @regexp_parser
            parse_rfc3164_regex(text, &block)
          else
            parse_rfc3164(text, &block)
          end
        end
      end

      def parse_rfc3164_regex(text, &block)
        idx = 0
        record = {}

        if @with_priority
          if (m = RFC3164_PRI_REGEXP.match(text))
            record['pri'] = Integer(m['pri'])
            idx = m.end(0)
          else
            yield(nil, nil)
            return
          end
        end

        i = idx - 1
        sq = false
        @space_count.times do
          while text[i + 1] == ' '.freeze
            sq = true
            i += 1
          end

          i = text.index(' '.freeze, i + 1)
        end

        time_str = sq ? text.slice(idx, i - idx).squeeze(' ') : text.slice(idx, i - idx)
        time = @mutex.synchronize { @time_parser.parse(time_str) }
        record['time'] = time_str

        parse_plain(time, text, i + 1, record, &block)
      end

      def parse_rfc5424_regex(text, &block)
        idx = 0
        record = {}

        if @with_priority
          if (m = RFC5424_PRI_REGEXP.match(text))
            m.names.each do |name|
              record[name] = m[name]
            end
            idx = m.end(0)
          else
            yield(nil, nil)
            return
          end
        end

        i = idx - 1
        sq = false
        @space_count_rfc5424.times {
          while text[i + 1] == ' '.freeze
            sq = true
            i += 1
          end

          i = text.index(' '.freeze, i + 1)
        }

        time_str = sq ? text.slice(idx, i - idx).squeeze(' '.freeze) : text.slice(idx, i - idx)
        time = @mutex.synchronize do
          begin
            @time_parser.parse(time_str)
          rescue Fluent::TimeParser::TimeParseError => e
            if @support_rfc5424_without_subseconds
              log.trace(e)
              @time_parser_rfc5424_without_subseconds.parse(time_str)
            else
              raise
            end
          end
        end

        record['time'] = time_str
        parse_plain(time, text, i + 1, record, &block)
      end

      # @param time [EventTime]
      # @param idx [Integer] note: this argument is needed to avoid string creation
      # @param record [Hash]
      def parse_plain(time, text, idx, record, &block)
        m = @regexp.match(text, idx)
        if m.nil?
          yield nil, nil
          return
        end

        unless @keep_time_key
          record.delete('time'.freeze)
        end

        m.names.each { |name|
          if value = m[name]
            case name
            when "message"
              value.chomp!
              record[name] = value
            else
              record[name] = value
            end
          end
        }

        if @estimate_current_event
          time ||= Fluent::EventTime.now
        end

        yield time, record
      end

      SPLIT_CHAR = ' '.freeze

      def parse_rfc3164(text, &block)
        pri = nil
        cursor = 0
        if @with_priority
          if text.start_with?('<'.freeze)
            i = text.index('>'.freeze, 1)
            if i < 2
              yield nil, nil
              return
            end
            pri = text.slice(1, i - 1).to_i
            cursor = i + 1
          else
            yield nil, nil
            return
          end
        end

        # header part
        time_size = 15 # skip Mmm dd hh:mm:ss
        time_end = text[cursor + time_size]
        if time_end == SPLIT_CHAR
          time_str = text.slice(cursor, time_size)
          cursor += 16 # time + ' '
        elsif time_end == '.'.freeze
          # support subsecond time
          i = text.index(SPLIT_CHAR, time_size)
          time_str = text.slice(cursor, i - cursor)
          cursor = i + 1
        else
          yield nil, nil
          return
        end

        i = text.index(SPLIT_CHAR, cursor)
        if i.nil?
          yield nil, nil
          return
        end
        host_size = i - cursor
        host = text.slice(cursor, host_size)
        cursor += host_size + 1

        record = {'host' => host}
        record['pri'] = pri if pri

        i = text.index(SPLIT_CHAR, cursor)

        # message part
        msg = if i.nil?  # for 'only non-space content case'
                text.slice(cursor, text.bytesize)
              else
                if text[i - 1] == ':'.freeze
                  if text[i - 2] == ']'.freeze
                    left_braket_pos = text.index('['.freeze, cursor)
                    record['ident'] = text.slice(cursor, left_braket_pos - cursor)
                    record['pid'] = text.slice(left_braket_pos + 1, i - left_braket_pos - 3) # remove '[' / ']:'
                  else
                    record['ident'] = text.slice(cursor, i - cursor - 1)
                  end
                  text.slice(i + 1, text.bytesize)
                else
                  if @support_colonless_ident
                    if text[i - 1] == ']'.freeze
                      left_braket_pos = text.index('['.freeze, cursor)
                      record['ident'] = text.slice(cursor, left_braket_pos - cursor)
                      record['pid'] = text.slice(left_braket_pos + 1, i - left_braket_pos - 2) # remove '[' / ']'
                    else
                      record['ident'] = text.slice(cursor, i - cursor)
                    end
                    text.slice(i + 1, text.bytesize)
                  else
                    text.slice(cursor, text.bytesize)
                  end
                end
              end
        msg.chomp!
        record['message'] = msg

        time = @time_parser.parse(time_str)
        record['time'] = time_str if @keep_time_key

        yield time, record
      end
    end
  end
end
