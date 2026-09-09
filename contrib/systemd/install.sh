#!/usr/bin/env bash
# install.sh — install the ecrawl-daily systemd units, wrapper script, and
# example config. Idempotent: an existing ecrawl-daily.conf is never overwritten.
#
# Usage:
#   sudo contrib/systemd/install.sh            # units + wrapper + example config
#   sudo contrib/systemd/install.sh --enable   # also: systemctl enable --now ecrawl-daily.timer
#
# Target dirs default to the canonical locations; override all three for
# staged/test installs (root not required when not using system paths):
#   EREPORT_CONFDIR=/tmp/etc EREPORT_LIBDIR=/tmp/lib EREPORT_UNITDIR=/tmp/units ./install.sh

set -euo pipefail

SRC=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

CONFDIR=${EREPORT_CONFDIR:-/etc/ereport}
LIBDIR=${EREPORT_LIBDIR:-/usr/local/lib/ereport}
UNITDIR=${EREPORT_UNITDIR:-/etc/systemd/system}
DEFAULT_LIBDIR=/usr/local/lib/ereport

ENABLE=0
for arg in "$@"; do
	case "$arg" in
	--enable) ENABLE=1 ;;
	-h|--help)
		sed -n '2,12p' "${BASH_SOURCE[0]}"
		exit 0
		;;
	*)
		echo "install.sh: unknown argument: $arg" >&2
		exit 2
		;;
	esac
done

using_defaults=0
if [[ $CONFDIR == /etc/ereport && $LIBDIR == "$DEFAULT_LIBDIR" && $UNITDIR == /etc/systemd/system ]]; then
	using_defaults=1
fi
if [[ $using_defaults -eq 1 && $EUID -ne 0 ]]; then
	echo "install.sh: installing to system paths; re-run as root (sudo $0)" >&2
	exit 1
fi

install -d "$CONFDIR" "$LIBDIR" "$UNITDIR"

if [[ -e $CONFDIR/ecrawl-daily.conf ]]; then
	echo "install.sh: keeping existing $CONFDIR/ecrawl-daily.conf"
else
	install -m0644 "$SRC/ecrawl-daily.conf.example" "$CONFDIR/ecrawl-daily.conf"
	echo "install.sh: installed example config -> $CONFDIR/ecrawl-daily.conf (edit it)"
fi

install -m0755 "$SRC/ecrawl-daily.sh" "$LIBDIR/ecrawl-daily.sh"
install -m0644 "$SRC/ecrawl-daily.service" "$UNITDIR/ecrawl-daily.service"
install -m0644 "$SRC/ecrawl-daily.timer" "$UNITDIR/ecrawl-daily.timer"

# Point ExecStart/Documentation at the wrapper when installed elsewhere.
if [[ $LIBDIR != "$DEFAULT_LIBDIR" ]]; then
	sed -i "s|$DEFAULT_LIBDIR|$LIBDIR|g" "$UNITDIR/ecrawl-daily.service"
fi

if command -v systemd-analyze >/dev/null; then
	if ! systemd-analyze verify "$UNITDIR/ecrawl-daily.service" "$UNITDIR/ecrawl-daily.timer"; then
		echo "install.sh: WARNING: systemd-analyze verify reported problems" >&2
	fi
fi

if [[ $EUID -eq 0 ]] && command -v systemctl >/dev/null; then
	systemctl daemon-reload
	if [[ $ENABLE -eq 1 ]]; then
		systemctl enable --now ecrawl-daily.timer
	fi
fi

echo "install.sh: done."
echo "  wrapper: $LIBDIR/ecrawl-daily.sh"
echo "  config:  $CONFDIR/ecrawl-daily.conf"
echo "  units:   $UNITDIR/ecrawl-daily.{service,timer}"
if [[ $using_defaults -eq 1 && $ENABLE -eq 0 ]]; then
	echo "  next:    edit the config, then: systemctl enable --now ecrawl-daily.timer"
fi
