# System Configuration

The `<system>` directive in Fluentd's configuration controls process-wide settings.

## Configuration Parameters

### `umask`

The `umask` parameter sets the process umask (file permission mask). This affects the default permissions of files created by Fluentd.

```
<system>
  umask 0022  # Allows r/w for owner, read for group/others
</system>
```

You can specify the umask value in octal format (e.g., `0022`, `0027`, `0077`). The meaning of umask values:
- `0022`: Allow read/write for owner, read for group/others
- `0027`: Allow read/write for owner, read for group, no access for others
- `0077`: Allow read/write for owner only, no access for group/others

Note: If not specified, Fluentd will use the system default umask.

### Examples

Restrictive umask (owner only):
```
<system>
  umask 0077
</system>
```

Standard umask:
```
<system>
  umask 0022
</system>
```

Group readable:
```
<system>
  umask 0027
</system>
```