#!/bin/sh
# Exercise the installer against a locally served release archive. No network
# release or credentials are needed; this is the clean-room package contract.
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
binary=${FORNIX_BINARY:-$repo_root/bin/fornix}
port=${FORNIX_PACKAGE_PORT:-18381}
work=$(mktemp -d "${TMPDIR:-/tmp}/fornix-package-smoke.XXXXXX")
server_pid=

cleanup() {
	if [ -n "$server_pid" ]; then
		kill "$server_pid" >/dev/null 2>&1 || true
		wait "$server_pid" 2>/dev/null || true
	fi
	rm -rf "$work"
}
trap cleanup EXIT HUP INT TERM

[ -x "$binary" ] || { echo "package smoke: missing executable $binary" >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo 'package smoke: python3 is required' >&2; exit 1; }
command -v tar >/dev/null 2>&1 || { echo 'package smoke: tar is required' >&2; exit 1; }
"$binary" --help | grep -F 'completion' >/dev/null
"$binary" completion bash | grep -F 'complete -F _fornix_complete fornix' >/dev/null
"$binary" doctor --json | grep -F '"checks"' >/dev/null

version=$("$binary" version --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["version"])')
os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$os" in
	darwin|linux) ;;
	*) echo "package smoke: unsupported host OS $os" >&2; exit 1 ;;
esac
arch=$(uname -m)
case "$arch" in
	x86_64|amd64) arch=amd64 ;;
	aarch64|arm64) arch=arm64 ;;
	*) echo "package smoke: unsupported host architecture $arch" >&2; exit 1 ;;
esac

release_dir="$work/release"
stage="$work/stage"
mkdir -p "$release_dir" "$stage"
cp "$binary" "$stage/fornix"
cp "$repo_root/LICENSE" "$stage/LICENSE"
cp "$repo_root/README.md" "$stage/README.md"
cp "$repo_root/THIRD_PARTY_NOTICES.md" "$stage/THIRD_PARTY_NOTICES.md"
archive_name="fornix_${version}_${os}_${arch}.tar.gz"
tar -czf "$release_dir/$archive_name" -C "$stage" fornix LICENSE README.md THIRD_PARTY_NOTICES.md
if command -v sha256sum >/dev/null 2>&1; then
	(cd "$release_dir" && sha256sum "$archive_name") > "$release_dir/checksums.txt"
else
	(cd "$release_dir" && shasum -a 256 "$archive_name") > "$release_dir/checksums.txt"
fi

"$repo_root/scripts/release/verify-artifacts.sh" "$release_dir"

python3 -m http.server "$port" --bind 127.0.0.1 --directory "$release_dir" >"$work/server.log" 2>&1 &
server_pid=$!
for _ in $(seq 1 30); do
	if curl -fsS "http://127.0.0.1:${port}/checksums.txt" >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
curl -fsS "http://127.0.0.1:${port}/checksums.txt" >/dev/null

install_dir="$work/install/bin"
FORNIX_VERSION="$version" FORNIX_RELEASE_BASE_URL="http://127.0.0.1:${port}" FORNIX_INSTALL_DIR="$install_dir" sh "$repo_root/scripts/install.sh"
installed_version=$("$install_dir/fornix" version --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["version"])')
[ "$installed_version" = "$version" ] || { echo 'package smoke: installed version mismatch' >&2; exit 1; }
"$install_dir/fornix" completion zsh | grep -F '#compdef fornix' >/dev/null

if command -v sha256sum >/dev/null 2>&1; then
	before_hash=$(sha256sum "$install_dir/fornix" | awk '{print $1}')
else
	before_hash=$(shasum -a 256 "$install_dir/fornix" | awk '{print $1}')
fi

# A checksum failure must not replace an already installed binary.
cp "$release_dir/checksums.txt" "$work/checksums.valid"
awk 'NR == 1 { $1="0000000000000000000000000000000000000000000000000000000000000000" } { print }' "$release_dir/checksums.txt" > "$work/checksums.invalid"
mv "$work/checksums.invalid" "$release_dir/checksums.txt"
if FORNIX_VERSION="$version" FORNIX_RELEASE_BASE_URL="http://127.0.0.1:${port}" FORNIX_INSTALL_DIR="$install_dir" sh "$repo_root/scripts/install.sh" >/dev/null 2>&1; then
	echo 'package smoke: checksum failure was accepted' >&2
	exit 1
fi
mv "$work/checksums.valid" "$release_dir/checksums.txt"
if command -v sha256sum >/dev/null 2>&1; then
	after_hash=$(sha256sum "$install_dir/fornix" | awk '{print $1}')
else
	after_hash=$(shasum -a 256 "$install_dir/fornix" | awk '{print $1}')
fi
[ "$after_hash" = "$before_hash" ] || { echo 'package smoke: failed install changed the existing binary' >&2; exit 1; }

# A symlink named fornix must be rejected before extraction or replacement.
unsafe_stage="$work/unsafe-stage"
mkdir -p "$unsafe_stage"
cp "$repo_root/LICENSE" "$unsafe_stage/LICENSE"
cp "$repo_root/README.md" "$unsafe_stage/README.md"
cp "$repo_root/THIRD_PARTY_NOTICES.md" "$unsafe_stage/THIRD_PARTY_NOTICES.md"
ln -s README.md "$unsafe_stage/fornix"
tar -czf "$release_dir/$archive_name" -C "$unsafe_stage" fornix LICENSE README.md THIRD_PARTY_NOTICES.md
if command -v sha256sum >/dev/null 2>&1; then
	(cd "$release_dir" && sha256sum "$archive_name") > "$release_dir/checksums.txt"
else
	(cd "$release_dir" && shasum -a 256 "$archive_name") > "$release_dir/checksums.txt"
fi
if "$repo_root/scripts/release/verify-artifacts.sh" "$release_dir" >/dev/null 2>&1; then
	echo 'package smoke: unsafe archive passed release verification' >&2
	exit 1
fi
if FORNIX_VERSION="$version" FORNIX_RELEASE_BASE_URL="http://127.0.0.1:${port}" FORNIX_INSTALL_DIR="$install_dir" sh "$repo_root/scripts/install.sh" >/dev/null 2>&1; then
	echo 'package smoke: unsafe archive was accepted by installer' >&2
	exit 1
fi
if command -v sha256sum >/dev/null 2>&1; then
	final_hash=$(sha256sum "$install_dir/fornix" | awk '{print $1}')
else
	final_hash=$(shasum -a 256 "$install_dir/fornix" | awk '{print $1}')
fi
[ "$final_hash" = "$before_hash" ] || { echo 'package smoke: unsafe archive changed the existing binary' >&2; exit 1; }

echo "package smoke: installer, checksum, and archive-safety checks passed for ${os}/${arch}"
