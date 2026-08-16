#!/usr/bin/env bash
# 板块突破监控 常驻守护（崩溃自动重启）。
# 用法：nohup ./run_breakout.sh >/dev/null 2>&1 &
set -e
cd "$(dirname "$0")"
while true; do
  echo "[$(date '+%F %T')] 启动板块突破监控"
  python3 sector_breakout_monitor.py || true
  echo "[$(date '+%F %T')] 进程退出，10 秒后重启"
  sleep 10
done
