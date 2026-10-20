#!/usr/bin/env bash
# rpm_build.sh — build the ereport RPMs for ONE EL release, in-container.
#
# The single source of truth for the rpmbuild recipe, called by:
#   make rpm RELEASE=9         one build; outside a container make wraps itself
#                              into an EL container, inside one calls into here
#   make rpms                  same, looped over $(RELEASES)
#   .github/workflows/rpm.yml  CI: per-release matrix, each leg `make rpm`
#
# Must run inside a Rocky/EL container: it installs build dependencies with
# dnf, and doing so on a real build host would mutate it. Detection covers:
# podman marks /run/.containerenv, docker marks /.dockerenv, systemd-nspawn
# marks /run/systemd/container; anything else needs an explicit opt-in
# (EREPORT_IN_BUILD_CONTAINER=1), which make sets inside the container it
# spawns from the host.
set -euo pipefail

rel="${1:?usage: rpm_build.sh <8|9|10>}"
case "$rel" in 8|9|10) ;; *) echo "unknown release: $rel" >&2; exit 2 ;; esac

if [ "${EREPORT_IN_BUILD_CONTAINER:-}" = 1 ] || [ -e /.dockerenv ] \
   || [ -e /run/.containerenv ] || [ -e /run/systemd/container ] \
   || grep -qE 'docker|podman|containerd|kubepods|nspawn' /proc/1/cgroup; then
    : # in a build container
else
    echo "not in a build container; run via 'make rpms' (or set EREPORT_IN_BUILD_CONTAINER=1)" >&2
    exit 2
fi

cd "$(dirname "$0")/../.."   # use from any repo subdirectory

# fuse-devel lives in CRB on EL9/EL10, BaseOS on EL8. redhat-rpm-config
# provides %optflags (and pulls annobin when gcc is present). systemd-rpm-macros
# carries the %systemd_* scriptlet macros: a real package on EL9+, a virtual
# provide of systemd on EL8. python3 is real on EL9 and virtual (python36) on
# EL8. This list must mirror the spec's BuildRequires for the enabled bconds
# (fuse on, jemalloc off, check on — same as CI's).
crb=""
if [ "$rel" != 8 ]; then crb="--enablerepo=crb"; fi
dnf -y $crb install gcc make rpm-build redhat-rpm-config \
                   systemd-rpm-macros \
                   libzstd-devel fuse-devel python3

# Per-release outputs are wiped so a rerun can never re-verify (and
# re-ship) stale RPMs from an earlier commit or the old build-local.sh;
# the SOURCES tarball is the only thing deliberately cached.
rm -rf rpmbuild/RPMS-el$rel rpmbuild/SRPMS-el$rel rpmbuild/artifacts/el$rel
mkdir -p rpmbuild/{SOURCES,SPECS,BUILD-el$rel,BUILDROOT-el$rel,RPMS-el$rel,SRPMS-el$rel,artifacts/el$rel}

sha="$(git rev-parse HEAD)"
gitdate="$(git show -s --format=%cd --date=format:%Y%m%d HEAD)"
tar="rpmbuild/SOURCES/ereport-$sha.tar.gz"
# Top-level name must match Source0: ereport-<sha>/, which %setup -n expects
[ -f "$tar" ] || git archive --prefix="ereport-$sha/" -o "$tar" HEAD
install -m 0644 contrib/rpm/ereport.spec rpmbuild/SPECS/ereport.spec

rpmbuild -ba \
    --define "_topdir $PWD/rpmbuild" \
    --define "_builddir $PWD/rpmbuild/BUILD-el$rel" \
    --define "_buildrootdir $PWD/rpmbuild/BUILDROOT-el$rel" \
    --define "_rpmdir $PWD/rpmbuild/RPMS-el$rel" \
    --define "_srcrpmdir $PWD/rpmbuild/SRPMS-el$rel" \
    --define "ereport_commit $sha" \
    --define "ereport_gitdate $gitdate" \
    rpmbuild/SPECS/ereport.spec

cp -a rpmbuild/RPMS-el$rel/*/*.rpm rpmbuild/SRPMS-el$rel/*.rpm \
      rpmbuild/artifacts/el$rel/

# Every RPM's NVR must carry this commit's shorthash (FPG snapshot naming)
rc=0
for r in rpmbuild/RPMS-el$rel/*/*.rpm rpmbuild/SRPMS-el$rel/*.rpm; do
    nvr="$(rpm -qp --qf '%{NAME}-%{VERSION}-%{RELEASE}\n' "$r")"
    echo "$r -> $nvr"
    case "$nvr" in
        *git"${sha:0:7}"*) ;;
        *) echo "FAIL: $nvr does not carry short sha ${sha:0:7}" >&2; rc=1 ;;
    esac
done
exit $rc
