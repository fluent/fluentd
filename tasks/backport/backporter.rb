require 'open-uri'
require 'json'
require 'optparse'
require 'logger'

class PullRequestBackporter

  def initialize
    @logger = Logger.new(STDOUT)
    @options = {
      upstream: "fluent/fluentd",
      branch: "v1.16",
      dry_run: false,
      log_level: Logger::Severity::INFO,
      remote: 'origin'
    }
  end

  def current_branch
    branch = IO.popen(["git", "branch", "--contains"]) do |io|
      io.read
    end
    branch.split.last
  end

  def parse_command_line(argv)
    opt = OptionParser.new
    opt.on('--upstream REPOSITORY',
           'Specify upstream repository (e.g. fluent/fluentd)') {|v| @options[:upstream] = v }
    opt.on('--branch BRANCH') {|v| @options[:branch] = v }
    opt.on('--dry-run') {|v| @options[:dry_run] = true }
    opt.on('--log-level LOG_LEVEL (e.g. debug,info)') {|v|
      @options[:log_level] = case v
                             when "error"
                               Logger::Severity::ERROR
                             when "warn"
                               Logger::Severity::WARN
                             when "debug"
                               Logger::Severity::DEBUG
                             when "info"
                               Logger::Severity::INFO
                             else
                               puts "unknown log level: <#{v}>"
                               exit 1
                             end
    }
    opt.on('--remote REMOTE') {|v| @options[:remote] = v }
    opt.parse!(argv)
  end

  def collect_backports
    backports = []
    pages = 5
    pages.times.each do |page|
      @logger.debug "Collecting backport information (#{page + 1}/#{pages})"
      URI.open("https://api.github.com/repos/#{@options[:upstream]}/pulls?state=closed&per_page=100&page=#{page+1}",
               "Accept" => "application/vnd.github+json",
               "Authorization" => "Bearer #{ENV['GITHUB_TOKEN']}",
               "X-GitHub-Api-Version" => "2022-11-28") do |request|
        JSON.parse(request.read).each do |pull_request|
          unless pull_request["labels"].empty?
            labels = pull_request["labels"].collect { |label| label["name"] }
            unless labels.include?("backport to #{@options[:branch]}")
              next
            end
            if labels.include?("backported")
              @logger.info "[DONE] \##{pull_request['number']} #{pull_request['title']} LABELS: #{pull_request['labels'].collect { |label| label['name'] }}"
              next
            end
            @logger.info "* \##{pull_request['number']} #{pull_request['title']} LABELS: #{pull_request['labels'].collect { |label| label['name'] }}"
            # merged into this commit
            @logger.debug "MERGE_COMMIT_SHA: #{pull_request['merge_commit_sha']}"
            body = pull_request["body"].gsub(/\*\*Which issue\(s\) this PR fixes\*\*: \r\n/,
                                             "**Which issue(s) this PR fixes**: \r\nBackport \##{pull_request['number']}\r\n")
            backports << {
              number: pull_request["number"],
              merge_commit_sha: pull_request["merge_commit_sha"],
              title: "Backport(#{@options[:branch]}): #{pull_request['title']} (\##{pull_request['number']})",
              body: body
            }
          end
        end
      end
    end
    backports
  end

  def create_pull_requests
    backports = collect_backports
    if backports.empty?
      @logger.info "No need to backport pull requests"
      return
    end

    failed = []
    original_branch = current_branch
    backports.each do |backport|
      @logger.info "Backport #{backport[:number]} #{backport[:title]}"
      if @options[:dry_run]
        @logger.info "DRY_RUN: PR was created: \##{backport[:number]} #{backport[:title]}"
        next
      end
      begin
        branch = "backport-to-#{@options[:branch]}/pr#{backport[:number]}"
        @logger.debug "git switch --create #{branch} --track #{@options[:remote]}/#{@options[:branch]}"
        IO.popen(["git", "switch", "--create",  branch, "--track",  "#{@options[:remote]}/#{@options[:branch]}"]) do |io|
          @logger.debug io.read
        end
        @logger.info `git branch`
        @logger.info "cherry-pick for #{backport[:number]}"
        @logger.debug "git cherry-pick --signoff #{backport[:merge_commit_sha]}"
        IO.popen(["git", "cherry-pick", "--signoff", backport[:merge_commit_sha]]) do |io|
          @logger.debug io.read
        end
        if $? != 0
          @logger.warn "Give up cherry-pick for #{backport[:number]}"
          @logger.debug `git cherry-pick --abort`
          failed << backport
          next
        else
          @logger.info "Push branch: #{branch}"
          @logger.debug `git push origin #{branch}`
        end

        upstream_repo = "/repos/#{@options[:upstream]}/pulls"
        owner = @options[:upstream].split('/').first
        head = "#{owner}:#{branch}"
        @logger.debug "Create pull request repo: #{upstream_repo} head: #{head} base: #{@options[:branch]}"
        IO.popen(["gh", "api", "--method", "POST",
                  "-H", "Accept: application/vnd.github+json",
                  "-H", "X-GitHub-Api-Version: 2022-11-28",
                  upstream_repo,
                  "-f", "title=#{backport[:title]}",
                  "-f", "body=#{backport[:body]}",
                  "-f", "head=#{head}",
                  "-f", "base=#{@options[:branch]}"]) do |io|
          json = JSON.parse(io.read)
          @logger.info "PR was created: #{json['url']}"
        end
      rescue => e
        @logger.error "ERROR: #{backport[:number]} #{e.message}"
      ensure
        IO.popen(["git", "checkout", original_branch]) do |io|
          @logger.debug io.read
        end
      end
    end
    failed.each do |backport|
      @logger.error "FAILED: #{backport[:number]} #{backport[:title]}"
    end
    failed.empty?
  end

  def run(argv)
    parse_command_line(argv)
    @logger.info("Target upstream: #{@options[:upstream]} target branch: #{@options[:branch]}")
    create_pull_requests
  end
end
