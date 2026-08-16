#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
板块突破实时盯盘（独立于「达标 6 规则」）
=========================================
监控 4 个板块，当某板块「当日平均涨幅」突破 +1% 时，立即推送微信（Server酱）。
- 实时轮询：默认每 30 秒一次（要「马上」就必须常驻进程，GitHub Actions 的 cron 做不到秒级）。
- 仅在交易时段（工作日 09:30-15:00）且行情日期为「今日」时判定，避免隔夜数据误报。
- 去重：当天每板块首次突破只推一次，次日自动重置。
- 推送：复用 push_utils（Server酱微信主通道 + 短信/电话兜底）。

用法：
  python sector_breakout_monitor.py            # 前台常驻（实时轮询，建议放轻量云/本机常开）
  python sector_breakout_monitor.py --once     # 单次检查（调试 / 配合外部调度）
  python sector_breakout_monitor.py --selftest # 逻辑自检（不联网）
"""
import os
import sys
import csv
import time
import json
import datetime

import push_utils as pu
import sector_qualification_analyzer as A   # 复用 get_spot_map 行情解析

BASE = os.path.dirname(os.path.abspath(__file__))
POOL_CSV = os.path.join(BASE, "all_sectors_pool.csv")
STATE_FILE = os.path.join(BASE, "sector_breakout_state.json")

POLL_INTERVAL = 30          # 轮询间隔（秒）
BREAKOUT_PCT = 1.0          # 板块平均涨幅突破阈值（+1%）
TRADING_START = (9, 30)
TRADING_END = (15, 0)


def read_pool():
    pool = []
    with open(POOL_CSV, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            pool.append((row["sector"].strip(), row["code"].strip(), row["name"].strip()))
    return pool


def is_trading_now():
    now = datetime.datetime.now()
    if now.weekday() >= 5:          # 周六、周日
        return False
    t = (now.hour, now.minute)
    return TRADING_START <= t <= TRADING_END


def compute_sector_avgs(pool, spot):
    from collections import defaultdict
    d = defaultdict(list)
    for sector, code, name in pool:
        v = spot.get(code)
        if not v:
            continue
        pct = v.get("pct")
        if pct is None:
            continue
        d[sector].append((name, pct))
    out = {}
    for s, lst in d.items():
        if not lst:
            continue
        avg = sum(p for _, p in lst) / len(lst)
        out[s] = (avg, len(lst), sorted(lst, key=lambda x: -x[1]))
    return out


def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"date": "", "pushed": []}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def today_str():
    return datetime.datetime.now().strftime("%Y%m%d")


def check_once(push=True, spot=None, pool=None):
    """单次检查。返回本次新突破的板块列表 [(sector, avg, n, ranked), ...]。"""
    date = today_str()
    state = load_state()
    if state.get("date") != date:
        state = {"date": date, "pushed": []}

    if pool is None:
        pool = read_pool()
    codes = [c for _, c, _ in pool]
    if spot is None:
        spot = A.get_spot_map(codes)
    if not spot:
        return []

    # 校验行情日期为今日：避免盘前/隔夜用上一交易日数据误判突破
    sample = next(iter(spot.values()))
    quote_date = (sample.get("time") or "")[:8]
    if quote_date and quote_date != date:
        return []

    avgs = compute_sector_avgs(pool, spot)
    newly = []
    for s, (avg, n, ranked) in avgs.items():
        if avg >= BREAKOUT_PCT and s not in state["pushed"]:
            newly.append((s, avg, n, ranked))
            state["pushed"].append(s)

    if newly:
        if push:
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            title = "🚀 板块突破 +1% 预警"
            lines = [f"以下板块当日平均涨幅已突破 +{BREAKOUT_PCT:.1f}%（{ts}）", ""]
            for s, avg, n, ranked in newly:
                lines.append(f"【{s}】平均 +{avg:.2f}%  （{n} 只成分股）")
                for nm, p in ranked[:3]:
                    lines.append(f"   ↳ {nm} {p:+.2f}%")
                lines.append("")
            desp = "\n".join(lines).strip()
            pu.push_all(title, desp)
        # 无论是否推送，都把去重结果持久化，保证当天不重复
        save_state(state)
    return newly


def sleep_to_next_open():
    now = datetime.datetime.now()
    target = now.replace(hour=9, minute=30, second=0, microsecond=0)
    if now >= target:
        target = target + datetime.timedelta(days=1)
    while target.weekday() >= 5:           # 跳过周末
        target = target + datetime.timedelta(days=1)
    secs = max(0, min((target - now).total_seconds(), 24 * 3600))
    print(f"[breakout] 非交易时段，休眠 {secs/3600:.1f}h 至 {target}", flush=True)
    time.sleep(secs)


def loop():
    print(f"[breakout] 板块突破实时盯盘启动（每 {POLL_INTERVAL}s 轮询，阈值 +{BREAKOUT_PCT:.1f}%）", flush=True)
    while True:
        if is_trading_now():
            try:
                newly = check_once(push=True)
                if newly:
                    print(f"[breakout] 已推送突破板块: {[x[0] for x in newly]}", flush=True)
            except Exception as e:
                print(f"[breakout] 检查异常: {e}", flush=True)
            time.sleep(POLL_INTERVAL)
        else:
            sleep_to_next_open()


def selftest():
    pool = [
        ("测试板块", "600000", "浦发银行"),
        ("测试板块", "600001", "测试B"),
        ("另一板块", "600002", "测试C"),
    ]
    td = today_str()
    spot = {
        "600000": {"name": "浦发银行", "pct": 1.5, "time": td + "093000"},
        "600001": {"name": "测试B", "pct": 0.5, "time": td + "093000"},
        "600002": {"name": "测试C", "pct": 0.2, "time": td + "093000"},
    }
    avgs = compute_sector_avgs(pool, spot)
    assert "测试板块" in avgs and abs(avgs["测试板块"][0] - 1.0) < 1e-9, avgs
    assert "另一板块" in avgs and abs(avgs["另一板块"][0] - 0.2) < 1e-9, avgs
    print("SELFTEST_OK avg_compute")

    # 去重逻辑：第一次有突破，第二次同一天不重复
    if os.path.exists(STATE_FILE):
        try:
            os.remove(STATE_FILE)
        except OSError:
            pass
    n1 = check_once(push=False, spot=spot, pool=pool)
    n2 = check_once(push=False, spot=spot, pool=pool)
    assert len(n1) == 1 and len(n2) == 0, (n1, n2)
    print("SELFTEST_OK dedup_per_day")

    # 非今日行情不判定
    spot_old = {k: {**v, "time": "20200101" + "093000"} for k, v in spot.items()}
    n3 = check_once(push=False, spot=spot_old, pool=pool)
    assert len(n3) == 0, n3
    print("SELFTEST_OK stale_quote_skip")

    if os.path.exists(STATE_FILE):
        try:
            os.remove(STATE_FILE)
        except OSError:
            pass
    print("ALL_SELFTEST_OK")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    elif "--once" in sys.argv:
        r = check_once(push=True)
        print("breakout check result:", [(x[0], round(x[1], 2)) for x in r])
    else:
        loop()
