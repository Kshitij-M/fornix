#!/bin/sh

# Keep Docker-backed smokes portable across developer machines and CI. The
# service runs on the host, while the fallback CLI runs in a container.

fornix_smoke_container_url() {
  printf '%s' "$1" | sed \
    -e 's#^http://127\.0\.0\.1#http://host.docker.internal#' \
    -e 's#^http://localhost#http://host.docker.internal#'
}

fornix_smoke_container_path() {
  smoke_path=$1
  smoke_repo_root=$2
  case "$smoke_path" in
    "$smoke_repo_root")
      printf '%s' /workspace
      ;;
    "$smoke_repo_root"/*)
      printf '/workspace/%s' "${smoke_path#"$smoke_repo_root"/}"
      ;;
    /workspace|/workspace/*)
      printf '%s' "$smoke_path"
      ;;
    *)
      printf '%s' "$smoke_path"
      ;;
  esac
}
