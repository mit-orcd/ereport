#!/usr/bin/env bash
# build-local.sh — build the ereport RPMs locally like CI's
# .github/workflows/rpm.yml (which only runs on pushes to main).
## Usage: contrib/rpm/build-local.sh [releases...]   # default: 8 9 10
## Output: rpmbuild/artifacts/el<rel>/ (gitignored).
# Images: quay.io/rockylinux/rockylinux. -i is required for the stdin script.

set -euo pipefail

if [ $# -gt 0 ]; then releases=("$@"); else releases=(8 9 10); fi

cd "$(dirname "$0")/../.."
mkdir -p rpmbuild/{SOURCES,SPECS,artifacts}

sha="$(git rev-parse HEAD)"
tarball="rpmbuild/SOURCES/ereport-$sha.tar.gz"
# Top-level name must match Source0: ereport-<sha>/, which %setup -n expects
[ -f "$tarball" ] || git archive --prefix="ereport-$sha/" -o "$tarball" HEAD
cp contrib/rpm/ereport.spec rpmbuild/SPECS/ereport.spec
gitdate="$(git show -s --format=%cd --date=format:%Y%m%d HEAD)"

container="$(command -v podman || command -v docker)"
[ -n "$container" ] || { echo "need podman or docker on PATH" >&2; exit 1; }

for rel in "${releases[@]}"; do
    # fuse-devel lives in CRB on EL9/EL10, BaseOS on EL8
    crb=""; [ "$rel" != 8 ] && crb="--enablerepo=crb"
    mkdir -p rpmbuild/{BUILD-el$rel,BUILDROOT-el$rel,RPMS-el$rel,SRPMS-el$rel} \
             rpmbuild/artifacts/el$rel
    "$container" run -i --rm -v "$PWD":/repo -w /repo \
        "quay.io/rockylinux/rockylinux:$rel" bash -ex <<EOF
dnf -y $crb install gcc make rpm-build redhat-rpm-config \\
    systemd-rpm-macros libzstd-devel fuse-devel python3

rpmbuild -ba \\
    --define "_topdir /repo/rpmbuild" \\
    --define "_builddir /repo/rpmbuild/BUILD-el$rel" \\
    --define "_buildrootdir /repo/rpmbuild/BUILDROOT-el$rel" \\
    --define "_rpmdir /repo/rpmbuild/RPMS-el$rel" \\
    --define "_srcrpmdir /repo/rpmbuild/SRPMS-el$rel" \\
    --define "ereport_commit $sha" \\
    --define "ereport_gitdate $gitdate" \\
    /repo/rpmbuild/SPECS/ereport.spec

cp -a rpmbuild/RPMS-el$rel/*/*.rpm rpmbuild/SRPMS-el$rel/*.rpm \\
      rpmbuild/artifacts/el$rel/
EOF
done
