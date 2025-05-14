$ErrorActionPreference = "Stop"
Set-PSDebug -Trace 1

$default_conf_path = (Resolve-Path fluent.conf).Path
$current_path = (Get-Location).Path
$log_path = "$current_path/fluentd.log"

ruby bin/fluentd --reg-winsvc i --reg-winsvc-fluentdopt "-c '$default_conf_path' -o '$log_path'"

# Test: must not start automatically
if ((Get-Service fluentdwinsvc).Status -ne "Stopped") {
    Write-Error "The service should not start automatically."
}

Start-Service fluentdwinsvc
Start-Sleep 30

# Test: the service should be running after started
if ((Get-Service fluentdwinsvc).Status -ne "Running") {
    Write-Error "The service should be running after started."
}

# Test: no warn/error/fatal logs
Get-ChildItem "*.log" | %{
    Get-Content $_
    if (Select-String -Path $_ -Pattern "[warn]", "[error]", "[fatal]" -SimpleMatch -Quiet) {
        Select-String -Path $_ -Pattern "[warn]", "[error]", "[fatal]" -SimpleMatch
        Write-Error "There are abnormal level logs in ${_}:"
    }
}

Stop-Service fluentdwinsvc
Start-Sleep 10 # Somehow it is possible that some processes stay alive for a while. (This could be not good behavior...)

# Test: status after stopped
if ((Get-Service fluentdwinsvc).Status -ne "Stopped") {
    Write-Error "The service should be in 'Stopped' status after stopped."
}
# Test: all Ruby processes should stop
$ruby_processes = Get-Process -name ruby -ErrorAction SilentlyContinue
if ($ruby_processes.Count -ne 0) {
    Write-Output $ruby_processes
    Write-Error "All Ruby processes should stop."
}

# Test: service should stop when the supervisor fails to launch
# https://github.com/fluent/fluentd/pull/4909
$test_setting = @'
<source>
  @type sample
  @id DUPLICATED_ID
  tag test
</source>
<match test>
  @type stdout
  @id DUPLICATED_ID
</match>
'@
Add-Content -Path "duplicated_id.conf" -Encoding UTF8 -Value $test_setting
ruby bin/fluentd --reg-winsvc-fluentdopt "-c '$current_path/duplicated_id.conf' -o '$log_path'"
Start-Service fluentdwinsvc
Start-Sleep 30
if ((Get-Service fluentdwinsvc).Status -ne "Stopped") {
    Write-Error "The service should be in 'Stopped' status when the supervisor fails to launch."
}
$ruby_processes = Get-Process -name ruby -ErrorAction SilentlyContinue
if ($ruby_processes.Count -ne 0) {
    Write-Output $ruby_processes
    Write-Error "All Ruby processes should stop."
}

ruby bin/fluentd --reg-winsvc u
Remove-Item $log_path
