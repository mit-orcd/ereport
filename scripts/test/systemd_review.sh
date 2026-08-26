#!/usr/bin/env bash
# Review harness: does contrib/systemd's ecrawl-daily.sh still work with the
# current ecrawl binary? Run on a compute node from the repo root.
set -uo pipefail

cd "$(dirname "$0")/../.."
ECRAWL=$PWD/ecrawl
SCRIPT=$PWD/contrib/systemd/ecrawl-daily.sh
T=$(mktemp -d "${TMPDIR:-/tmp}/systemd-review.XXXXXX")
trap 'rm -rf "$T"' EXIT
fails=0

check() { # check <label> <condition...>
	local label=$1; shift
	if "$@"; then echo "PASS: $label"; else echo "FAIL: $label"; fails=$((fails+1)); fi
}

# fixture tree
mkdir -p "$T/tree/a/b"
for i in 0 1 2 3 4; do head -c $((100+i)) /dev/zero >"$T/tree/a/f$i.dat"; done
for i in 0 1 2;   do head -c $((50+i))  /dev/zero >"$T/tree/a/b/g$i.dat"; done
ln -s a/f0.dat "$T/tree/link"

echo "== 0. ecrawl no longer accepts --record-root =="
"$ECRAWL" --record-root /foo "$T/tree" "$T/out_x" >/dev/null 2>"$T/rr.err"
check "--record-root rejected (exit 2)" [ $? -eq 2 ]
check "unknown-option message" grep -q "unknown option: --record-root" "$T/rr.err"

echo "== 1. daily run, current 2-column config =="
cat >"$T/good.conf" <<EOF
ECRAWL_BIN=$ECRAWL
---jobs---
$T/tree	$T/out1
EOF
bash "$SCRIPT" "$T/good.conf" >"$T/good.log" 2>&1
check "exit 0" [ $? -eq 0 ]
check "uid_shard_*.bin written" bash -c "compgen -G '$T/out1/uid_shard_*.bin' >/dev/null"
check "crawl_manifest.txt written" [ -f "$T/out1/crawl_manifest.txt" ]
check "uid.txt written" [ -f "$T/out1/uid.txt" ]
check "gid.txt written" [ -f "$T/out1/gid.txt" ]
check "manifest has start_path" grep -q "^start_path=$T/tree" "$T/out1/crawl_manifest.txt"
check "manifest has NO record_root" bash -c "! grep -q '^record_root=' '$T/out1/crawl_manifest.txt'"
check "stdout names the artifacts the cleanup pattern expects" \
	grep -q "uid_shards=" "$T/out1/crawl_manifest.txt"

echo "== 2. legacy 3-column config still runs, warns =="
printf 'ECRAWL_BIN=%s\n---jobs---\n%s\t%s\t/legacy/root\n' "$ECRAWL" "$T/tree" "$T/out2" >"$T/legacy.conf"
bash "$SCRIPT" "$T/legacy.conf" >"$T/legacy.log" 2>&1
check "exit 0" [ $? -eq 0 ]
check "deprecation warning printed" grep -q "ignoring record_root '/legacy/root'" "$T/legacy.log"
check "crawl still produced shards" bash -c "compgen -G '$T/out2/uid_shard_*.bin' >/dev/null"
check "no --record-root passed to ecrawl" bash -c "! grep -q -- '--record-root /legacy' <(grep 'ecrawl-daily: ' '$T/legacy.log' | grep "$ECRAWL")"

echo "== 3. failing job: exit 1 + artifact cleanup =="
mkdir -p "$T/out3"
touch "$T/out3/uid_shard_000.bin" "$T/out3/uid_shard_000.bin.ckpt" "$T/out3/crawl_manifest.txt" "$T/out3/uid.txt" "$T/out3/gid.txt"
printf 'ECRAWL_BIN=%s\n---jobs---\n%s\t%s\n' "$ECRAWL" "$T/does-not-exist" "$T/out3" >"$T/bad.conf"
bash "$SCRIPT" "$T/bad.conf" >"$T/bad.log" 2>&1
check "exit 1" [ $? -eq 1 ]
check "failure reported" grep -q "ecrawl failed for start_path=" "$T/bad.log"
check "stale artifacts cleaned after failure" bash -c "compgen -G '$T/out3/uid_shard_*.bin' >/dev/null; [ \$? -ne 0 ] && [ ! -f '$T/out3/crawl_manifest.txt' ]"

echo "== 4. local RSYNC_DEST sync + post-sync local cleanup =="
mkdir -p "$T/mirror"
printf 'ECRAWL_BIN=%s\nRSYNC_DEST=%s\n---jobs---\n%s\t%s\n' "$ECRAWL" "$T/mirror" "$T/tree" "$T/out4" >"$T/rsync.conf"
bash "$SCRIPT" "$T/rsync.conf" >"$T/rsync.log" 2>&1
check "exit 0" [ $? -eq 0 ]
check "rsync ran to mirror/out4" [ -d "$T/mirror/out4" ]
check "remote has manifest" [ -f "$T/mirror/out4/crawl_manifest.txt" ]
check "local shards removed after successful rsync" bash -c "! compgen -G '$T/out4/uid_shard_*.bin' >/dev/null"

echo "== 5. unit files parse (if systemd-analyze is around) =="
if command -v systemd-analyze >/dev/null; then
	# verify checks that ExecStart resolves, and we cannot install to
	# /usr/local here, so verify copies whose ExecStart points at a staged
	# executable. (systemd 239 has no verify --root.)
	mkdir -p "$T/units"
	install -m0755 "$SCRIPT" "$T/units/ecrawl-daily.sh"
	sed "s|/usr/local/lib/ereport/ecrawl-daily.sh|$T/units/ecrawl-daily.sh|" \
		contrib/systemd/ecrawl-daily.service >"$T/units/ecrawl-daily.service"
	cp contrib/systemd/ecrawl-daily.timer "$T/units/"
	systemd-analyze verify "$T/units/ecrawl-daily.service" "$T/units/ecrawl-daily.timer" \
		>"$T/verify.log" 2>&1
	check "systemd-analyze verify clean" [ $? -eq 0 ]
	grep -E "ecrawl-daily" "$T/verify.log" | sed 's/^/  verify: /' | head -10
else
	echo "SKIP: systemd-analyze not installed"
fi

echo
if [ "$fails" -eq 0 ]; then echo "ALL CHECKS PASSED"; else echo "$fails CHECK(S) FAILED"; exit 1; fi
