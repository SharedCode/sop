# Local command daemon

`sop-daemon` accepts approved commands from the SOP web UI on loopback port
9090. Configure the exact programs callers may use before starting it:

```sh
go run ./cmd/sop-daemon -allow-executables echo
```

For installed operational tools, use an explicit list such as
`-allow-executables kubectl,terraform`. The environment variable
`SOP_DAEMON_ALLOW_EXECUTABLES` supplies the same list when the flag is omitted.
An empty list or any program that cannot be resolved prevents startup. Earlier
versions allowed every executable when the list was omitted; existing launch
commands must now specify the programs they need.

Each entry is matched exactly against the request's `executable`, or the first
word of the legacy `command` field. Allowing `echo` does not allow `/bin/echo`,
`./echo`, or another path alias. If a full path is configured, callers must send
that exact path. Both request forms use the same policy:

```sh
curl -sS http://127.0.0.1:9090/api/execute \
  -H 'Content-Type: application/json' \
  -d '{"executable":"echo","args":["hello"]}'
```

The daemon resolves configured names through its startup `PATH`, converts them
to absolute paths, and resolves symlinks before listening. Requests select those
stored paths. Later `PATH` changes or changes to a configured symlink cannot
select a different executable. Restart after intentionally changing a tool's
location. Protect the resolved files and their parent directories against
replacement; this policy does not pin file contents or sandbox child processes.
The configured entry is preserved as `argv[0]`, so multicall programs such as
BusyBox retain their selected applet while the executable path stays pinned.

Arguments are passed directly without a shell. Pipes, redirects, substitutions,
and chaining are literal text unless the selected program interprets them.
Allowing a shell, interpreter, or a tool with execution flags grants callers its
full capabilities, including any code execution it supports. Only allow tools
whose arguments you trust callers to control. This is an executable policy,
not a restriction on individual subcommands, files, credentials, or operations.

Browser callers must also match `-allowed-origins`, which defaults to
`http://localhost:8080,http://127.0.0.1:8080`. Keep those origins trusted.
Use `-token` or `SOP_DAEMON_TOKEN` to require the
`X-SOP-Daemon-Token` header from every execution request, including local clients
without an Origin header. The token is removed from child process environments.
Commands retain the daemon user's other permissions and environment variables.

`-command-timeout` defaults to 60 seconds. Each output stream is capped at
1 MiB, with truncation reported in the JSON response. Run `sop-daemon -h` for
all startup options.
