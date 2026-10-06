#!/usr/bin/env bash
# Installs the collector on a Raspberry Pi (or any systemd Linux).
# Usage: sudo deploy/install.sh /mnt/usb    (USB mount point, already in /etc/fstab)
set -euo pipefail

MOUNT="${1:-/mnt/usb}"
SRC="$(dirname "$(readlink -f "$0")")/.."
PREFIX=/opt/deribit-collector

apt-get install -y g++ cmake git libssl-dev libzstd-dev ca-certificates rclone

cmake -S "$SRC" -B /tmp/deribit-collector-build -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/deribit-collector-build -j"$(nproc)"
install -D -m 755 /tmp/deribit-collector-build/deribit_collector "$PREFIX/deribit_collector"

id collector >/dev/null 2>&1 || useradd --system --create-home --shell /usr/sbin/nologin collector
install -d -o collector -g collector "$MOUNT/deribit-data"

for f in deribit-collector.service deribit-upload.service deribit-upload.timer; do
	sed "s#/mnt/usb#$MOUNT#g" "$SRC/deploy/$f" > "/etc/systemd/system/$f"
done
[ -f /etc/default/deribit-upload ] || echo 'REMOTE=remote:bucket/deribit' > /etc/default/deribit-upload

systemctl daemon-reload
systemctl enable --now deribit-collector.service
echo "Collector running. Configure rclone as user 'collector' (sudo -u collector rclone config),"
echo "set REMOTE in /etc/default/deribit-upload, then: systemctl enable --now deribit-upload.timer"
