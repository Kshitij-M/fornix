#!/bin/sh
# Verify the safety and completeness of a Fornix release artifact directory.
# This is intentionally independent of GoReleaser so it can run in CI, in a
# release job, and by a maintainer reviewing a downloaded release.
set -eu

artifact_dir=dist
require_matrix=false

fail() {
	printf 'fornix artifact verifier: %s\n' "$1" >&2
	exit 1
}

while [ "$#" -gt 0 ]; do
	case "$1" in
		--require-matrix) require_matrix=true ;;
		--help|-h)
			printf 'usage: %s [--require-matrix] [artifact-directory]\n' "$0"
			exit 0
			;;
		-*) fail "unknown option: $1" ;;
		*) [ "$artifact_dir" = dist ] || fail 'only one artifact directory may be supplied'; artifact_dir=$1 ;;
	esac
	shift
done

[ -d "$artifact_dir" ] || fail "artifact directory does not exist: $artifact_dir"
[ -f "$artifact_dir/checksums.txt" ] || fail 'checksums.txt is missing'
command -v tar >/dev/null 2>&1 || fail 'tar is required'

sha256() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{print $1}'
		return
	fi
	command -v shasum >/dev/null 2>&1 || fail 'sha256sum or shasum is required'
	shasum -a 256 "$1" | awk '{print $1}'
}

archives=$(find "$artifact_dir" -maxdepth 1 -type f -name 'fornix_*.tar.gz' -print | sort)
canonical_count=0
for archive in $archives; do
	name=${archive##*/}
	case "$name" in
		fornix_tools_*) continue ;;
	esac
	canonical_count=$((canonical_count + 1))

	checksum_count=$(awk -v expected_name="$name" '$2 == expected_name { count++ } END { print count + 0 }' "$artifact_dir/checksums.txt")
	[ "$checksum_count" -eq 1 ] || fail "expected exactly one checksum entry for $name"
	expected=$(awk -v expected_name="$name" '$2 == expected_name { print $1; exit }' "$artifact_dir/checksums.txt")
	case "$expected" in
		????????????????????????????????????????????????????????????????) ;;
		*) fail "invalid SHA-256 checksum for $name" ;;
	esac
	actual=$(sha256 "$archive")
	[ "$actual" = "$expected" ] || fail "checksum mismatch for $name"

	if ! tar -tzf "$archive" | awk 'BEGIN { bad=0; binary=0; license=0; readme=0; notices=0 } {
		name=$0
		if (name == "" || name == "." || name ~ /^\// || name ~ /(^|\/)\.\.($|\/)/ || name ~ /\/$/) bad=1
		if (name == "fornix") binary++
		if (name == "LICENSE") license++
		if (name == "README.md") readme++
		if (name == "THIRD_PARTY_NOTICES.md") notices++
	} END { if (bad || binary != 1 || license != 1 || readme != 1 || notices != 1) exit 1 }'; then
		fail "unsafe, incomplete, or unexpected archive paths in $name"
	fi
	if ! tar -tvzf "$archive" | awk 'BEGIN { bad=0 } { if (substr($0, 1, 1) != "-") bad=1 } END { exit bad }'; then
		fail "archive contains a non-regular file: $name"
	fi

	extract_dir=$(mktemp -d "${TMPDIR:-/tmp}/fornix-artifact.XXXXXX")
	trap 'rm -rf "$extract_dir"' EXIT HUP INT TERM
	tar -xzf "$archive" -C "$extract_dir"
	[ -f "$extract_dir/fornix" ] && [ ! -L "$extract_dir/fornix" ] || fail "archive binary is not a regular file: $name"
	for metadata in "$extract_dir/README.md" "$extract_dir/LICENSE" "$extract_dir/THIRD_PARTY_NOTICES.md"; do
		if grep -E -I -q 'sk-[A-Za-z0-9_-]{20,}|fornix_(db|bootstrap)_[A-Za-z0-9]{16,}|BEGIN [A-Z ]*PRIVATE KEY' "$metadata"; then
			fail "possible credential or private key in release metadata: ${metadata##*/}"
		fi
	done
	rm -rf "$extract_dir"
	trap - EXIT HUP INT TERM
done

[ "$canonical_count" -gt 0 ] || fail 'no canonical Fornix archives were found'

if [ "$require_matrix" = true ]; then
	for target in linux_amd64 linux_arm64 darwin_amd64 darwin_arm64; do
		found=0
		for archive in $archives; do
			name=${archive##*/}
			case "$name" in
				fornix_tools_*) continue ;;
				fornix_*_${target}.tar.gz) found=$((found + 1)) ;;
			esac
		done
		[ "$found" -eq 1 ] || fail "expected exactly one canonical archive for $target"
	done
fi

printf 'Verified %s canonical Fornix archive(s) in %s\n' "$canonical_count" "$artifact_dir"
