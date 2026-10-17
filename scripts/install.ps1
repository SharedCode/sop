# Installs sop-mcp-server, the Joltrin barrier for MCP agents, and registers it
# with the agent CLIs found on this machine (Claude Code, Codex, Gemini CLI).
# This is the Windows version of install.sh.
#
#   PowerShell:  irm https://raw.githubusercontent.com/SharedCode/joltrin/master/scripts/install.ps1 | iex
#   cmd.exe:     powershell -NoProfile -ExecutionPolicy Bypass -Command "irm https://raw.githubusercontent.com/SharedCode/joltrin/master/scripts/install.ps1 | iex"
#
# It downloads the latest release binary, checks its SHA-256 against the
# release's checksum file, and puts it in %USERPROFILE%\.joltrin\bin. It does
# not need administrator rights.
#
# Set these environment variables first (PowerShell: $env:JOLTRIN_VERIFY = "1",
# cmd.exe: set JOLTRIN_VERIFY=1 and run the command in the same window):
#
#   JOLTRIN_VERIFY=1     also verify the binary's signed build provenance with
#                        the GitHub CLI (gh, signed in). It does not rely on the
#                        checksum file, needs a release from v5.11.0 on, and adds
#                        a few seconds, so it is off by default
#   JOLTRIN_VERSION=TAG  install that release instead of the latest, e.g. v5.11.0
#   JOLTRIN_NO_SETUP=1   install only, do not register with any agent
#   JOLTRIN_BIN_DIR=DIR  install somewhere other than %USERPROFILE%\.joltrin\bin
#
# Trust: the script, the binary and the checksum file all come from the same
# GitHub release, so the checksum catches a damaged or swapped download but not
# a compromised release. If that matters to you, set JOLTRIN_VERIFY=1, read this
# script first, pin a version, or build from source.

function Install-Joltrin {
  $ErrorActionPreference = 'Stop'
  $ProgressPreference = 'SilentlyContinue'
  # Windows PowerShell 5.1 does not offer TLS 1.2 to GitHub by default.
  [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

  if ($env:JOLTRIN_VERSION) {
    $defaultBase = "https://github.com/SharedCode/joltrin/releases/download/$env:JOLTRIN_VERSION"
  } else {
    $defaultBase = 'https://github.com/SharedCode/joltrin/releases/latest/download'
  }
  $base = if ($env:JOLTRIN_BASE_URL) { $env:JOLTRIN_BASE_URL } else { $defaultBase }
  $dest = if ($env:JOLTRIN_BIN_DIR) { $env:JOLTRIN_BIN_DIR } else { Join-Path $HOME '.joltrin\bin' }

  # A 32-bit PowerShell on a 64-bit machine reports x86, the real value is in PROCESSOR_ARCHITEW6432.
  $machine = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
  switch ($machine) {
    'AMD64' { $arch = 'amd64' }
    'ARM64' { $arch = 'arm64' }
    default { throw "install: no prebuilt binary for $machine" }
  }

  $name = "sop-mcp-server-windows-$arch.exe"
  $sums = 'sop-mcp-server-SHA256SUMS'
  $tmp = Join-Path ([IO.Path]::GetTempPath()) ("joltrin-" + [Guid]::NewGuid().ToString('N'))
  New-Item -ItemType Directory -Path $tmp | Out-Null
  try {
    Write-Host "Downloading $name"
    try {
      Invoke-WebRequest -UseBasicParsing -Uri "$base/$name" -OutFile (Join-Path $tmp $name)
      Invoke-WebRequest -UseBasicParsing -Uri "$base/$sums" -OutFile (Join-Path $tmp $sums)
    } catch {
      throw "install: download failed from $base"
    }

    $line = Get-Content (Join-Path $tmp $sums) | Where-Object { $_ -match "\s\*?$([regex]::Escape($name))$" } | Select-Object -First 1
    if (-not $line) { throw "install: $name is not in the checksum file" }
    $want = ($line -split '\s+')[0]
    $got = (Get-FileHash -Algorithm SHA256 -Path (Join-Path $tmp $name)).Hash
    if ($got -ne $want) { throw 'install: checksum does not match, nothing was installed' }

    if ($env:JOLTRIN_VERIFY -eq '1') {
      if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
        throw 'install: JOLTRIN_VERIFY=1 needs the GitHub CLI (gh), signed in with gh auth login'
      }
      Write-Host 'Verifying build provenance'
      & gh attestation verify (Join-Path $tmp $name) --repo SharedCode/joltrin *> $null
      if ($LASTEXITCODE -ne 0) {
        throw 'install: build provenance could not be verified, nothing was installed. It needs gh signed in and a release from v5.11.0 on'
      }
    }

    New-Item -ItemType Directory -Force -Path $dest | Out-Null
    $exe = Join-Path $dest 'sop-mcp-server.exe'
    Move-Item -Force -Path (Join-Path $tmp $name) -Destination $exe
    Write-Host "Installed $exe"

    if ($env:JOLTRIN_NO_SETUP -eq '1') {
      Write-Host "Register it with: & '$exe' setup --apply"
    } else {
      & $exe setup --apply
      if ($LASTEXITCODE -ne 0) { throw "install: setup exited with code $LASTEXITCODE" }
    }
  } finally {
    Remove-Item -Recurse -Force -Path $tmp -ErrorAction SilentlyContinue
  }
}

Install-Joltrin
