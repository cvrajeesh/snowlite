#!/bin/sh
# Install snowlite-server — downloads the pre-built binary for your platform.
# Usage: curl -fsSL https://raw.githubusercontent.com/cvrajeesh/snowlite/main/install.sh | sh
set -e

REPO="cvrajeesh/snowlite"
BINARY="snowlite-server"

# Detect OS
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$OS" in
  darwin)
    ARTIFACT="${BINARY}-macos-universal"
    ;;
  linux)
    case "$ARCH" in
      x86_64)          ARTIFACT="${BINARY}-linux-x86_64" ;;
      aarch64 | arm64) ARTIFACT="${BINARY}-linux-aarch64" ;;
      *)
        echo "Unsupported architecture: $ARCH" >&2
        exit 1
        ;;
    esac
    ;;
  *)
    echo "Unsupported OS: $OS" >&2
    echo "For Windows, use install.ps1 instead." >&2
    exit 1
    ;;
esac

# Resolve download URL (latest release)
URL="https://github.com/${REPO}/releases/latest/download/${ARTIFACT}"

echo "Downloading ${ARTIFACT} from ${URL} ..."

if command -v curl > /dev/null 2>&1; then
  curl -fsSL -o "$BINARY" "$URL"
elif command -v wget > /dev/null 2>&1; then
  wget -q -O "$BINARY" "$URL"
else
  echo "Error: curl or wget is required." >&2
  exit 1
fi

chmod +x "$BINARY"

echo ""
echo "snowlite-server installed → ./${BINARY}"
echo ""
echo "Run it:"
echo "  ./${BINARY}              # default port 8765"
echo "  ./${BINARY} --port 9000 # custom port"
