#!/usr/bin/env bash
# EC2 更换/重启后主机名会变，Chrome 仍认为「另一台机器」占着默认配置，导致 GUI 里无法打开。
# 在确认没有需要保留的未保存 Chrome 标签页后执行本脚本，再启动 Chrome。
set -euo pipefail
PROFILE="${HOME}/.config/google-chrome"
if [[ ! -d "$PROFILE" ]]; then
  echo "no Chrome profile at $PROFILE"
  exit 0
fi
pkill -9 -f '[g]oogle-chrome' 2>/dev/null || true
sleep 1
rm -f "$PROFILE/SingletonLock" "$PROFILE/SingletonSocket" "$PROFILE/SingletonCookie"
rm -rf /tmp/com.google.Chrome.* 2>/dev/null || true
echo "removed Chrome singleton locks under $PROFILE"
