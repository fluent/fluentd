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
      RFC3164_CAPTURES = RFC3164_WITHOUT_TIME_AND_PRI_REGEXP.names.freeze
      RFC3164_PRI_REGEXP = /^<(?<pri>[0-9]{1,3})>/

      RFC5424_WITHOUT_TIME_AND_PRI_REGEXP = /(?<host>[!-~]{1,255}) (?<ident>[!-~]{1,48}) (?<pid>[!-~]{1,128}) (?<msgid>[!-~]{1,32}) (?<extradata>(?:\-|(?:\[.*?(?<!\\)\])+))(?: (?<message>.+))?\z/m
      RFC5424_CAPTURES = RFC5424_WITHOUT_TIME_AND_PRI_REGEXP.names.freeze
      RFC5424_PRI_REGEXP = /^<(?<pri>\d{1,3})>\d\d{0,2}\s/

      config_set_default :time_format, "%b %d %H:%M:%S"
      desc 'If the incoming logs have priority prefix, e.g. <9>, set true'
      config_param :with_priority, :bool, default: false
      desc 'Specify protocol format'
      config_param :message_format, :enum, list: [:rfc3164, :rfc5424, :auto], default: :rfc3164
      desc 'Specify time format for event time for rfc5424 protocol'
      config_param :rfc5424_time_format, :string, default: "%Y-%m-%dT%H:%M:%S.%L%z"
      desc 'The parser type used to parse syslog message'
      config_param :parser_engine, :enum, list: [:regexp, :string], default: :regexp, alias: :parser_type
      desc 'support colonless ident in string parser'
      config_param :support_colonless_ident, :bool, default: true

      def initialize
        super
        @mutex = Mutex.new
        @regexp = nil
        @regexp3164 = nil
        @regexp5424 = nil
        @regexp_parser = nil
        @time_parser_rfc3164 = nil
        @time_parser_rfc5424 = nil
        @space_count_rfc3164 = nil
        @space_count_rfc5424 = nil
        @skip_space_count_rfc3164 = false
        @skip_space_count_rfc5424 = false
        @time_parser_rfc5424_without_subseconds = nil
      end

      def configure(conf)
        super

        @regexp_parser = @parser_engine == :regexp
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
                    setup_time_parser_3164(@time_format)
                    RFC3164_WITHOUT_TIME_AND_PRI_REGEXP
                  when :rfc5424
                    if @regexp_parser
                      class << self
                        alias_method :parse, :parse_rfc5424_regex
                      end
                    else
                      class << self
                        alias_method :parse, :parse_rfc5424
                      end
                    end
                    @time_format = @rfc5424_time_format unless conf.has_key?('time_format')
                    setup_time_parser_5424(@time_format)
                    RFC5424_WITHOUT_TIME_AND_PRI_REGEXP
                  when :auto
                    class << self
                      alias_method :parse, :parse_auto
                    end
                    setup_time_parser_3164(@time_format)
                    setup_time_parser_5424(@rfc5424_time_format)
                    nil
                  end

        if @regexp_parser
          @regexp3164 = RFC3164_WITHOUT_TIME_AND_PRI_REGEXP
          @regexp5424 = RFC5424_WITHOUT_TIME_AND_PRI_REGEXP
        end
      end

      def setup_time_parser_3164(time_fmt)
        @time_parser_rfc3164 = time_parser_create(format: time_fmt)
        if ['%b %d %H:%M:%S', '%b %d %H:%M:%S.%N'].include?(time_fmt)
          @skip_space_count_rfc3164 = true
        end
        @space_count_rfc3164 = time_fmt.squeeze(' ').count(' ') + 1
      end

      def setup_time_parser_5424(time_fmt)
        @time_parser_rfc5424 = time_parser_create(format: time_fmt)
        @time_parser_rfc5424_without_subseconds = time_parser_create(format: "%Y-%m-%dT%H:%M:%S%z")
        @skip_space_count_rfc5424 = time_fmt.count(' ').zero?
        @space_count_rfc5424 = time_fmt.squeeze(' ').count(' ') + 1
      end

      # this method is for tests
      def patterns
        {'format' => @regexp, 'time_format' => @time_format}
      end

      def parse(text)
        # This is overwritten in configure
      end

      def parse_auto(text, &block)
        if REGEXP_DETECT_RFC5424.match?(text)
          if @regexp_parser
            parse_rfc5424_regex(text, &block)
          else
            parse_rfc5424(text, &block)
          end
        else
          if @regexp_parser
            parse_rfc3164_regex(text, &block)
          else
            parse_rfc3164(text, &block)
          end
        end
      end

      SPLIT_CHAR = ' '.freeze

      def parse_rfc3164_regex(text, &block)
        idx = 0
        record = {}

        if @with_priority
          if RFC3164_PRI_REGEXP.match?(text)
            v = text.index('>')
            record['pri'] = text[1..v].to_i # trim `<` and ``>
            idx = v + 1
          else
            yield(nil, nil)
            return
          end
        end

        i = idx - 1
        sq = false
        @space_count_rfc3164.times do
          while text[i + 1] == SPLIT_CHAR
            sq = true
            i += 1
          end

          i = text.index(SPLIT_CHAR, i + 1)
        end

        time_str = sq ? text.slice(idx, i - idx).squeeze(SPLIT_CHAR) : text.slice(idx, i - idx)
        time = @mutex.synchronize { @time_parser_rfc3164.parse(time_str) }
        if @keep_time_key
          record['time'] = time_str
        end

        parse_plain(@regexp3164, time, text, i + 1, record, RFC3164_CAPTURES, &block)
      end

      def parse_rfc5424_regex(text, &block)
        idx = 0
        record = {}

        if @with_priority
          if (m = RFC5424_PRI_REGEXP.match(text))
            record['pri'] = m['pri'].to_i
            idx = m.end(0)
          else
            yield(nil, nil)
            return
          end
        end

        i = idx - 1
        sq = false
        @space_count_rfc5424.times {
          while text[i + 1] == SPLIT_CHAR
            sq = true
            i += 1
          end

          i = text.index(SPLIT_CHAR, i + 1)
        }

        time_str = sq ? text.slice(idx, i - idx).squeeze(SPLIT_CHAR) : text.slice(idx, i - idx)
        time = @mutex.synchronize do
          begin
            @time_parser_rfc5424.parse(time_str)
          rescue Fluent::TimeParser::TimeParseError => e
            log.trace(e)
            @time_parser_rfc5424_without_subseconds.parse(time_str)
          end
        end

        if @keep_time_key
          record['time'] = time_str
        end
        parse_plain(@regexp5424, time, text, i + 1, record, RFC5424_CAPTURES, &block)
      end

      # @param time [EventTime]
      # @param idx [Integer] note: this argument is needed to avoid string creation
      # @param record [Hash]
      # @param capture_list [Array] for performance
      def parse_plain(re, time, text, idx, record, capture_list, &block)
        m = re.match(text, idx)
        if m.nil?
          yield nil, nil
          return
        end

        capture_list.each { |name|
          if value = (m[name] rescue nil)
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

        if @skip_space_count_rfc3164
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
        else
          i = cursor - 1
          sq = false
          @space_count_rfc3164.times do
            while text[i + 1] == SPLIT_CHAR
              sq = true
              i += 1
            end
            i = text.index(SPLIT_CHAR, i + 1)
          end

          time_str = sq ? text.slice(idx, i - cursor).squeeze(SPLIT_CHAR) : text.slice(cursor, i - cursor)
          cursor = i + 1
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

        time = @time_parser_rfc3164.parse(time_str)
        record['time'] = time_str if @keep_time_key

        yield time, record
      end

      NILVALUE = '-'.freeze

      def parse_rfc5424(text, &block)
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
            i = text.index(SPLIT_CHAR, i)
            cursor = i + 1
          else
            yield nil, nil
            return
          end
        end

        # timestamp part
        if @skip_space_count_rfc5424
          i = text.index(SPLIT_CHAR, cursor)
          time_str = text.slice(cursor, i - cursor)
          cursor = i + 1
        else
          i = cursor - 1
          sq = false
          @space_count_rfc5424.times do
            while text[i + 1] == SPLIT_CHAR
              sq = true
              i += 1
            end
            i = text.index(SPLIT_CHAR, i + 1)
          end

          time_str = sq ? text.slice(idx, i - cursor).squeeze(SPLIT_CHAR) : text.slice(cursor, i - cursor)
          cursor = i + 1
        end

        # Repeat same code for the performance

        # host part
        i = text.index(SPLIT_CHAR, cursor)
        unless i
          yield nil, nil
          return
        end
        slice_size = i - cursor
        host = text.slice(cursor, slice_size)
        cursor += slice_size + 1

        # ident part
        i = text.index(SPLIT_CHAR, cursor)
        unless i
          yield nil, nil
          return
        end
        slice_size = i - cursor
        ident = text.slice(cursor, slice_size)
        cursor += slice_size + 1

        # pid part
        i = text.index(SPLIT_CHAR, cursor)
        unless i
          yield nil, nil
          return
        end
        slice_size = i - cursor
        pid = text.slice(cursor, slice_size)
        cursor += slice_size + 1

        # msgid part
        i = text.index(SPLIT_CHAR, cursor)
        unless i
          yield nil, nil
          return
        end
        slice_size = i - cursor
        msgid = text.slice(cursor, slice_size)
        cursor += slice_size + 1

        record = {'host' => host, 'ident' => ident, 'pid' => pid, 'msgid' => msgid}
        record['pri'] = pri if pri

        # extradata part
        ed_start = text[cursor]
        if ed_start == NILVALUE
          record['extradata'] = NILVALUE
          cursor += 1
        else
          start = cursor
          i = text.index('] '.freeze, cursor)
          extradata = if i
                        diff = i + 1 - start # calculate ']' position
                        cursor += diff
                        text.slice(start, diff)
                      else  # No message part case
                        cursor = text.bytesize
                        text.slice(start, cursor)
                      end
          extradata.tr!("\\".freeze, ''.freeze)
          record['extradata'] = extradata
        end

        # message part
        if cursor != text.bytesize
          msg = text.slice(cursor + 1, text.bytesize)
          msg.chomp!
          record['message'] = msg
        end

        time = begin
                 @time_parser_rfc5424.parse(time_str)
               rescue Fluent::TimeParser::TimeParseError => e
                 @time_parser_rfc5424_without_subseconds.parse(time_str)
               end
        record['time'] = time_str if @keep_time_key

        yield time, record
      end
    end
  end
end
