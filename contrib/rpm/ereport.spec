# ereport — https://github.com/mit-orcd/ereport
#
# C tools that crawl filesystem metadata once into compact binary records,
# then answer questions from the records (HTML reports, path search,
# find/du-style queries) without walking the tree again.
#
# One spec, three packages:
#   ereport          ecrawl, ereport, ereport_index, ecrawl_query, edump,
#                    (ecrawl_mount when built with FUSE) and eserve.py
#   ereport-systemd  the ecrawl-daily timer + wrapper + noreplace config
#   ereport-docs     upstream docs/ tree
#
# Upstream has no release tags yet, so this packages a git snapshot using
# FPG snapshot naming: Version: 0^<yyyymmdd>git<7-char sha> (the caret sorts
# snapshots above a bare "0" and below "0.1", so a future first release
# upgrades cleanly). The pinned values below are only the fallback for hand
# builds; CI (.github/workflows/rpm.yml) overrides them with the commit
# actually being built, so every published RPM carries that commit's
# shorthash:
#   rpmbuild --define "ereport_commit <full sha>" \
#            --define "ereport_gitdate <commit date, yyyymmdd>"
#
# Build options:
#   --without fuse    skip ecrawl_mount (needs FUSE 2.x headers: fuse-devel,
#                     BaseOS on EL8, CRB on EL9/EL10; use on distros that
#                     really do ship fuse3 only)
#   --with jemalloc   link native tools against jemalloc. Off by default:
#                     upstream documents that without it builds are
#                     byte-identical glibc-malloc builds, and a build host
#                     that happens to have jemalloc-devel must not change the
#                     output. When enabled, runtime needs libjemalloc.so.2.
#   --without check   skip the test suite in %%check

%bcond_without fuse
%bcond_with    jemalloc
%bcond_without check

# Snapshot pin. Hand builds use the fallback values; CI
# (.github/workflows/rpm.yml) overrides them with the commit actually being
# built so every published RPM's NVR carries that commit's shorthash:
#   rpmbuild --define "ereport_commit <full sha>" \
#            --define "ereport_gitdate <yyyymmdd>"
# Hand-building from a git checkout (what CI does):
#   sha=$(git rev-parse HEAD)
#   git archive --prefix="ereport-$sha/" -o SOURCES/ereport-$sha.tar.gz HEAD
#   rpmbuild --define "ereport_commit $sha" \
#            --define "ereport_gitdate $(git show -s --format=%cd --date=format:%Y%m%d HEAD)" \
#            .../ereport.spec
%global commit       %{?ereport_commit}%{!?ereport_commit:7308ed67c352ec3157e1ee3a8672a35cb45dbf25}
%global shortcommit  %(c="%{commit}"; echo "$c" | cut -c1-7)
%global gitdate      %{?ereport_gitdate}%{!?ereport_gitdate:20260930}

# Fallback for build hosts whose rpm does not define %_unitdir (RHEL/Fedora
# get it from redhat-rpm-config; plain rpm installs may not).
%if 0%{?_unitdir:1}
%else
%global _unitdir %{_prefix}/lib/systemd/system
%endif

Name:           ereport
# FPG snapshot naming (Fedora packaging guidelines, "Snapshots"): upstream
# has never chosen a version, so Version is 0, and the snapshot field
# ^<yyyymmdd>git<7-char sha> rides in the Version tag after a caret. The
# caret makes post-release snapshots sort higher than the bare version and
# lower than 0.1, so a future first real release (Version: 0.1) upgrades
# cleanly over any snapshot.
Version:        0^%{gitdate}git%{shortcommit}
Release:        1%{?dist}
Summary:        Filesystem crawl and reporting tools (ecrawl, ereport and friends)

License:        MIT
URL:            https://github.com/mit-orcd/ereport
Source0:        https://github.com/mit-orcd/ereport/archive/%{commit}/%{name}-%{commit}.tar.gz
# sha256 b68b251b086bf114e5d4f82406b6999bd5c3484154acd537c609a3fee9801487

BuildRequires:  gcc
BuildRequires:  make
BuildRequires:  pkgconfig(libzstd)
%if %{with fuse}
BuildRequires:  pkgconfig(fuse)
%endif
%if %{with jemalloc}
BuildRequires:  pkgconfig(jemalloc)
%endif
%if %{with check}
BuildRequires:  python3
%endif
BuildRequires:  systemd-rpm-macros

# eserve.py (report web server, stdlib-only) needs an interpreter.
Requires:       python3

%description
ereport is a set of C tools that crawl filesystem metadata once into compact
binary records, then answer questions from those records without walking the
tree again: static HTML reports with an age x size heat map and sunburst,
trigram path search over billions of paths, and du/find-style queries.

This package provides the ecrawl, ereport, ereport_index, ecrawl_query and
edump binaries (ecrawl_mount too when built with FUSE support), plus
eserve.py, the web server for the generated reports.

%package systemd
Summary:        Daily ecrawl timer for ereport
BuildArch:      noarch
Requires:       %{name} = %{?epoch:%{epoch}:}%{version}-%{release}
Requires(post): systemd
Requires(preun): systemd
Requires(postun): systemd
Recommends:     rsync

%description systemd
Runs ecrawl daily on the directories listed in
/etc/ereport/ecrawl-daily.conf and, optionally, rsyncs each crawl output to a
central host. Same wrapper, units and config format as upstream's
contrib/systemd/. Install, edit the config, then:

    systemctl enable --now ecrawl-daily.timer

The timer is not enabled by default; the service runs only from the timer.

%package docs
Summary:        Documentation for ereport
BuildArch:      noarch

%description docs
Upstream documentation for ereport: tool reference, environment variables,
build and deploy notes, performance measurements, binary-format layout and
screenshots. See /usr/share/doc/ereport/README.RPM for this package's file
layout.

%prep
%setup -q -n %{name}-%{commit}

# Upstream's contrib/systemd/install.sh targets /usr/local/lib/ereport and
# /opt/ereport. An RPM install uses distro paths instead; rewrite the unit
# and the example config to match this package's layout (same substitution
# install.sh performs when EREPORT_LIBDIR is overridden). sed -i.bak (then
# dropping the .bak) rather than plain -i, so this also runs under BSD sed.
for f in contrib/systemd/ecrawl-daily.service \
         contrib/systemd/ecrawl-daily.conf.example; do
    sed -i.bak \
        -e 's|/usr/local/lib/ereport|%{_libexecdir}/ereport|g' \
        -e 's|/opt/ereport/ecrawl|%{_bindir}/ecrawl|g' \
        "$f" && rm -f "$f.bak"
done

%build
# The Makefile pins its own CFLAGS and ignores the environment; pass the
# distro flags on the make command line instead (keeps -g, -O2, hardening
# and debuginfo working). -pthread is required. On the command line, make
# ignores the Makefile's per-OS appends, so Darwin builders must include
# -D_DARWIN_C_SOURCE in optflags; on Linux nothing extra is needed.
%if %{with jemalloc}
make %{?_smp_mflags} CFLAGS="%{?optflags} -pthread"
%else
# jemalloc off: JEMALLOC_LIBS= (empty) forces the glibc-malloc build even
# when the build host happens to have jemalloc-devel installed.
make %{?_smp_mflags} CFLAGS="%{?optflags} -pthread" JEMALLOC_LIBS=
%endif

%check
%if %{with check}
# Unit tests + the ecrawl/ereport integration harness on a synthetic tree.
# ecrawl_mount live-mount checks self-skip when the binary is absent or
# /dev/fuse is unavailable.
make check CFLAGS="%{?optflags} -pthread"
%endif

%install
rm -rf %{buildroot}

# Native tools and the report web server.
install -m 0755 -d %{buildroot}%{_bindir}
for bin in ecrawl ereport ereport_index ecrawl_query edump; do
    install -m 0755 -p "$bin" %{buildroot}%{_bindir}/
done
%if %{with fuse}
install -m 0755 -p ecrawl_mount %{buildroot}%{_bindir}/
%endif
install -m 0755 -p eserve.py %{buildroot}%{_bindir}/eserve.py

# Daily-crawl units, wrapper and the ZFS/autofs path-rewrite helper.
install -m 0755 -d %{buildroot}%{_libexecdir}/ereport
install -m 0755 -p contrib/systemd/ecrawl-daily.sh %{buildroot}%{_libexecdir}/ereport/
install -m 0755 -p scripts/ecrawl-zfs-autofs-map.sh %{buildroot}%{_libexecdir}/ereport/
install -m 0755 -d %{buildroot}%{_unitdir}
install -m 0644 -p contrib/systemd/ecrawl-daily.service \
    contrib/systemd/ecrawl-daily.timer %{buildroot}%{_unitdir}/
install -m 0755 -d %{buildroot}%{_sysconfdir}/ereport
install -m 0644 -p contrib/systemd/ecrawl-daily.conf.example \
    %{buildroot}%{_sysconfdir}/ereport/ecrawl-daily.conf

# Documentation (docs subpackage; kept out of %doc so the subpackage owns it
# explicitly and no %doc/%license dir overlap is possible).
install -m 0755 -d %{buildroot}%{_docdir}/%{name}
cp -a docs %{buildroot}%{_docdir}/%{name}/docs
install -m 0644 -p README.md %{buildroot}%{_docdir}/%{name}/
cat > %{buildroot}%{_docdir}/%{name}/README.RPM <<'EOF'
ereport RPM layout
==================

Binaries (package: ereport)
  %{_bindir}/ecrawl           parallel crawler; writes uid-sharded records
  %{_bindir}/ereport          static HTML report generator
  %{_bindir}/ereport_index    trigram path-search index (make/query)
  %{_bindir}/ecrawl_query    du/find-style queries over a crawl
  %{_bindir}/edump            recreate a crawl as a synthetic tree
  %{_bindir}/eserve.py        HTTP server for reports + the search box
  (when built with FUSE: %{_bindir}/ecrawl_mount, a read-only FUSE view of
   a crawl; Linux only)

Quick start
  ecrawl /path/to/tree crawl-out
  ereport mtime crawl-out
  ereport_index --make crawl-out
  eserve.py --bind 127.0.0.1 --port 8000 ./all_users
  # then open http://127.0.0.1:8000/index.html?search=1

eserve.py finds ereport_index on PATH; both are in %{_bindir}.

Documentation (package: ereport-docs)
  %{_docdir}/%{name}/docs/    tool reference, formats, performance, ...

Daily crawl (package: ereport-systemd)
  units:    %{_unitdir}/ecrawl-daily.service, ecrawl-daily.timer
  wrapper:  %{_libexecdir}/ereport/ecrawl-daily.sh
  config:   /etc/ereport/ecrawl-daily.conf (noreplace; edit before enabling)

  After editing the job list (and optional RSYNC_DEST):
      systemctl enable --now ecrawl-daily.timer

  The timer is disabled by default. ECRAWL_BIN already points at
  %{_bindir}/ecrawl, and PATH_REWRITE_MAP (ZFS/autofs relabeling) at
  %{_libexecdir}/ereport/ecrawl-zfs-autofs-map.sh.

Note: upstream's contrib/systemd/install.sh installs to /usr/local/lib/ereport
and expects ecrawl under /opt/ereport; this package uses the distro paths
above instead.
EOF

%post systemd
%systemd_post ecrawl-daily.timer

%preun systemd
%systemd_preun ecrawl-daily.timer

%postun systemd
%systemd_postun ecrawl-daily.timer

%files
%license LICENSE
%{_bindir}/ecrawl
%{_bindir}/ereport
%{_bindir}/ereport_index
%{_bindir}/ecrawl_query
%{_bindir}/edump
%if %{with fuse}
%{_bindir}/ecrawl_mount
%endif
%{_bindir}/eserve.py

%files systemd
%dir %{_libexecdir}/ereport
%{_libexecdir}/ereport/ecrawl-daily.sh
%{_libexecdir}/ereport/ecrawl-zfs-autofs-map.sh
%{_unitdir}/ecrawl-daily.service
%{_unitdir}/ecrawl-daily.timer
%dir %{_sysconfdir}/ereport
%config(noreplace) %{_sysconfdir}/ereport/ecrawl-daily.conf

%files docs
%{_docdir}/%{name}

%changelog
* Wed Oct 07 2026 Lincoln Bryant <lincolnb@mit.edu> - 0^20260930git7308ed6-1
- Initial package of git snapshot 7308ed6 (2026-09-30): ecrawl, ereport,
  ereport_index, ecrawl_query, edump and eserve.py
- FPG snapshot naming: Version 0^<yyyymmdd>git<shortsha>, Release 1
- ereport-systemd subpackage: ecrawl-daily.{service,timer}, wrapper and
  noreplace config, paths rewritten from upstream's /usr/local/lib/ereport
- ereport-docs subpackage carrying upstream docs/
- CI (.github/workflows/rpm.yml) overrides ereport_commit/ereport_gitdate so
  each published RPM's NVR carries the commit actually built
