# Security Policy

## Supported Versions

|  Version  |     Supported      |
|-----------|--------------------|
| 1.19.x    | :white_check_mark: |
| 1.18.x    | :x:                |
| 1.17.x    | :x:                |
| 1.16.x    | :x:                |
| <= 1.15.x | :x:                |

## Reporting a Vulnerability

Please post your vulnerability report from the following page:
https://github.com/fluent/fluentd/security/advisories

> [!CAUTION]
> In above contact form, we accept about **ONLY** Fluentd's vulnerability, in contrast, **REJECT** your vulnerability report about Fluentd derivative products such as fluent-package, Fluentd container images and Fluentd kubernetes deamonset images and so on. There are appropriate contact forms for every these derivative products. See the following notice.

> [!NOTE]
> If you use fluent-package, please check [fluent-package-builder](https://github.com/fluent/fluent-package-builder/blob/master/SECURITY.md) and report it there.

> [!NOTE]
> If you use a Docker image of Fluentd, please check [Fluentd Docker Image](https://github.com/fluent/fluentd-docker-image/blob/master/SECURITY.md) and report it there.


## Out of Scope / Non-Vulnerabilities

Before reporting a vulnerability, please check if it falls under the following expected behaviors, design choices, or configuration issues. Reports regarding these items will be closed immediately as **not a bug** or **out of scope**.

### 1. `in_debug_agent` and arbitrary code execution (`eval`)

* **Description:** The `in_debug_agent` plugin is a developer tool designed specifically for debugging a running Fluentd process via dRuby. It requires explicit activation in the local configuration file and is disabled by default.
* **Why it is not a vulnerability:** The ability to execute arbitrary Ruby code (`eval`) is **by design**. It functions similarly to `gdbserver`, allowing maintainers to inspect internal process states. It does not introduce a security risk unless an administrator explicitly enables it and exposes the port to an untrusted network, which constitutes a severe misconfiguration rather than a flaw in Fluentd itself.

### 2. `record_transformer` with `enable_ruby`

* **Description:** The `enable_ruby` option in the `record_transformer` plugin allows administrators to use Ruby expressions within the configuration file to transform log records dynamically.
* **Why it is not a vulnerability:** Executing the Ruby code block defined in the configuration file is **by design**. It does not allow arbitrary code execution from untrusted log inputs unless the user explicitly writes insecure expressions (e.g., passing untrusted input directly into another dynamic evaluation nested inside the block). If an attacker can modify the local Fluentd configuration file to enable this feature and inject malicious Ruby code, the system is already fully compromised at the OS/infrastructure level (Post-Exploitation). We do not accept RCE reports that rely on the ability to modify the local configuration file.

### 3. ReDoS (Regular Expression Denial of Service) reproducible only on Ruby 3.2 or older

* **Description:** Potential algorithmic complexity issues in regular expressions that could lead to High CPU usage or Denial of Service on older Ruby runtimes.
* **Why it is out of scope:** Fluentd officially supports modern Ruby environments. Our primary distribution channels including the official `fluent-package` and official Docker images ship with **Ruby 3.3 or later (e.g., Ruby 3.4)**, which includes built-in, robust mitigation against ReDoS at the language level (such as matching timeouts and engine-level optimizations). We do not accept ReDoS reports that only impact legacy Ruby versions (Ruby 3.2 or older) unless the issue can be demonstrated to cause a practical denial of service on Ruby 3.3+.  
  However, if you find inefficient regular expressions, we welcome them as standard bug reports or performance improvement suggestions via regular GitHub Issues rather than security advisories.
