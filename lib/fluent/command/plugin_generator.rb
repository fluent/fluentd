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

require "optparse"
require "pathname"
require "fileutils"
require "erb"

class FluentPluginGenerator
  attr_reader :type, :name
  attr_reader :license

  LICENSES_MAP = {
    "Apache-2.0" => "Apache License, Version 2.0"
  }

  def initialize(argv = ARGV)
    @argv = argv
    @parser = prepare_parser

    @license = "Apache-2.0"
    @overwrite_all = false
  end

  def call
    parse_options!
    FileUtils.mkdir_p(gem_name)
    Dir.chdir(gem_name) do
      copy_license
      template_directory.find do |path|
        next if path.directory?
        next if path.fnmatch?("*/preambles/*")
        next if path.fnmatch?("*/licenses/*")
        dest_dir = path.dirname.sub(/\A#{Regexp.quote(template_directory.to_s)}\/?/, "")
        dest_file = dest_filename(path)
        if path.extname == ".erb"
          if path.fnmatch?("*/plugin/*")
            next unless path.basename.fnmatch?("*#{type}*")
          end
          template(path, dest_dir + dest_file)
        else
          file(path, dest_dir + dest_file)
        end
      end
      pid = spawn("git", "init", ".")
      Process.wait(pid)
    end
  end

  private

  def template_directory
    (Pathname(__dir__) + "../../../templates/new_gem").realpath
  end

  def template_file(filename)
    template_directory + filename
  end

  def template(source, dest)
    dest.dirname.mkpath
    contents = ERB.new(source.read, nil, "-").result(binding)
    label = create_label(dest, contents)
    puts "\t#{label} #{dest}"
    if label == "conflict"
      return unless overwrite?(dest)
    end
    File.write(dest, contents)
  end

  def file(source, dest)
    label = create_label(dest, source.read)
    puts "\t#{label} #{dest}"
    if label == "conflict"
      return unless overwrite?(dest)
    end
    FileUtils.cp(source, dest)
  end

  def prepare_parser
    @parser = OptionParser.new
    @parser.banner = <<BANNER
Usage: fluent-plugin-generate [options] <type> <name>
BANNER

    @parser.on("--[no-]license=NAME", "Specify license name") do |v|
      @license = v
    end
    @parser
  end

  def parse_options!
    @parser.parse!(@argv)
    unless @argv.size == 2
      raise ArgumentError, "Missing arguments"
    end
    @type, @name = @argv
  rescue => e
    puts "#{e.class}:#{e.message}"
    puts @parser.help
    exit(false)
  end

  def user_name
    v = `git config --get user.name`.chomp
    v.empty? ? "TODO: Write your name" : v
  end

  def user_email
    v = `git config --get user.email`.chomp
    v.empty? ? "TODO: Write your email" : v
  end

  def gem_name
    "fluent-plugin-#{name}"
  end

  def class_name
    "#{name.capitalize}#{type.capitalize}"
  end

  def plugin_filename
    case type
    when "input"
      "in_#{name}.rb"
    when "output"
      "out_#{name}.rb"
    else
      "#{type}_#{name}.rb"
    end
  end

  def test_filename
    case type
    when "input"
      "test_in_#{name}.rb"
    when "output"
      "test_out_#{name}.rb"
    else
      "test_#{type}_#{name}.rb"
    end
  end

  def dest_filename(path)
    case path.to_s
    when %r!\.gemspec!
      "#{gem_name}.gemspec"
    when %r!lib/fluent/plugin!
      plugin_filename
    when %r!test/plugin!
      test_filename
    else
      path.basename.sub_ext("")
    end
  end

  def preamble
    return "" unless license
    preamble_file = template_file("preambles/#{license}.erb")
    return "" unless preamble_file.exist?
    src = preamble_file.read
    ERB.new(src, nil, "-").result(binding).lines.map {|line| "# #{line}".gsub(/ +$/, "") }.join
  end

  def license_full_name
    LICENSES_MAP[license]
  end

  def copy_license
    # in gem_name directory
    return unless license
    puts "License: #{license}"
    license_file = template_file("licenses/#{license}.txt")
    return unless license_file.exist?
    file(license_file, Pathname("LICENSE"))
  end

  def create_label(dest, contents)
    if dest.exist?
      if dest.read == contents
        "identical"
      else
        "conflict"
      end
    else
      "create"
    end
  end

  def overwrite?(dest)
    return true if @overwrite_all
    loop do
      print "Overwrite #{dest}? (enter \"h\" for help) [Ynaqh]"
      answer = $stdin.gets.chomp
      return true if /\Ay\z/i =~ answer || answer.empty?
      case answer
      when "n"
        return false
      when "a"
        @overwrite_all = true
        return true
      when "q"
        exit
      when "h"
        puts <<HELP
\tY - yes, overwrite
\tn - no, do not overwrite
\ta - all, overwrite this and all others
\tq - quite, abort
\th - help, show this help
HELP
      end
      puts "Retrying..."
    end
  end
end
