#!/bin/sh
# Install the verified Fornix CLI for the current macOS or Linux user.
#
# The convenience path is intentionally small, but it downloads the release
# checksum manifest before extracting anything and installs atomically. It
# never accepts or persists provider credentials.
set -eu

repository="${FORNIX_REPOSITORY:-Kshitij-M/fornix}"
version="${FORNIX_VERSION:-latest}"
install_dir="${FORNIX_INSTALL_DIR:-${XDG_BIN_HOME:-${HOME}/.local/bin}}"

fail() {
	printf 'fornix installer: %s\n' "$1" >&2
	exit 1
}

command -v curl >/dev/null 2>&1 || fail 'curl is required; install it and retry'
command -v tar >/dev/null 2>&1 || fail 'tar is required; install it and retry'

os=$(uname -s 2>/dev/null || true)
case "$os" in
	Darwin) os=darwin ;;
	Linux) os=linux ;;
	*) fail "unsupported operating system: ${os:-unknown}; supported systems are macOS and Linux" ;;
esac

arch=$(uname -m 2>/dev/null || true)
case "$arch" in
	x86_64|amd64) arch=amd64 ;;
	aarch64|arm64) arch=arm64 ;;
	*) fail "unsupported architecture: ${arch:-unknown}; supported architectures are amd64 and arm64" ;;
esac

if [ "$version" = latest ]; then
	tag=$(curl -fsSL "https://api.github.com/repos/${repository}/releases/latest" | \
		sed -n 's/^[[:space:]]*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)
	[ -n "$tag" ] || fail 'could not resolve the latest release; set FORNIX_VERSION explicitly'
	version=${tag#v}
else
	version=${version#v}
	tag="v${version}"
fi

archive="fornix_${version}_${os}_${arch}.tar.gz"
base_url="https://github.com/${repository}/releases/download/${tag}"
temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/fornix-install.XXXXXX")
cleanup() { rm -rf "$temp_dir"; }
trap cleanup EXIT HUP INT TERM

curl -fsSL "${base_url}/checksums.txt" -o "${temp_dir}/checksums.txt" || fail "could not download checksums for ${tag}"
curl -fsSL "${base_url}/${archive}" -o "${temp_dir}/${archive}" || fail "could not download ${archive}"

expected=$(awk -v name="$archive" '$2 == name { print $1; exit }' "${temp_dir}/checksums.txt")
[ -n "$expected" ] || fail "checksum entry for ${archive} was not found"

if command -v sha256sum >/dev/null 2>&1; then
	actual=$(sha256sum "${temp_dir}/${archive}" | awk '{print $1}')
elif command -v shasum >/dev/null 2>&1; then
	actual=$(shasum -a 256 "${temp_dir}/${archive}" | awk '{print $1}')
else
	fail 'sha256sum or shasum is required to verify the release'
fi
[ "$actual" = "$expected" ] || fail "checksum verification failed for ${archive}"

# Reject absolute and parent-traversal names before extraction.
if tar -tzf "${temp_dir}/${archive}" | awk 'BEGIN { bad=0 } /^\// || /(^|\/)\.\.($|\/)/ { bad=1 } END { exit bad }'; then
	:
else
	fail "archive contains an unsafe path"
fi

mkdir -p "$install_dir"
extract_dir="${temp_dir}/extract"
mkdir "$extract_dir"
tar -xzf "${temp_dir}/${archive}" -C "$extract_dir"
[ -f "${extract_dir}/fornix" ] || fail 'release archive does not contain the fornix binary'

chmod 0755 "${extract_dir}/fornix"
target="${install_dir}/fornix"
staged="${target}.new.$$"
cp "${extract_dir}/fornix" "$staged"
chmod 0755 "$staged"
mv -f "$staged" "$target"

printf 'Installed fornix %s (%s/%s) to %s\n' "$version" "$os" "$arch" "$target"
printf 'Run: %s start\n' "$target"
