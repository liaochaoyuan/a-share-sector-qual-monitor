#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
板块突破 + 个股突破 双路实时盯盘（GitHub Actions 5分钟级 cron 版）
================================================================
数据源：腾讯实时行情 qt.gtimg.cn（标准库 urllib，零第三方依赖）
监控对象：all_sectors_pool.csv 中的 4 板块 39 只个股
触发规则：
  A. 板块突破：某板块「当日平均涨幅」首次 >= +1.0%  → 立即推送（当天每板块去重一次）
  B. 个股突破：某只个股「当日涨幅」首次 >= +3.0%（或涨停）→ 立即推送（当天每股票去重一次）
推送：复用 push_utils（Server酱微信主通道；SENDKEY 来自环境变量 SERVERCHAN_SENDKEY / push_config.json）
状态持久化：breakout_state.json（提交回仓库，跨 cron 调用保持去重；每日北京时间自动重置）
运行：
  python breakout_monitor.py --once       # 单次检查（配合 GitHub Actions 5分钟 cron）
  python breakout_monitor.py --test-push  # 发送一条测试告警（验证通道是否通）
  python breakout_monitor.py --selftest   # 逻辑自检（不联网）
说明：本工具只做「规则条件判断」，不做任何行情预测。
"""
import os
import re
import sys
import csv
import json
import datetime
import urllib.request

import push_utils as pu

BASE = os.path.dirname(os.path.abspath(__file__))
POOL_CSV = os.path.join(BASE, "all_sectors_pool.csv")
STATE_FILE = os.path.join(BASE, "breakout_state.json")

SECTOR_BREAKOUT_PCT = 1.0   # 板块平均涨幅突破阈值（+1%）
STOCK_BREAKOUT_PCT = 3.0    # 个股涨幅突破阈值（+3%）；另含涨停判定
TRADING_START = (9, 25)
TRADING_END = (15, 0)
SPOT_URL = "https://qt.gtimg.cn/q="


# ======================================================================
# 工具
# ======================================================================
def beijing_now():
    """返回北京时间（UTC+8）。GitHub runner 时钟为 UTC，必须显式加 8 小时。"""
    return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)


def today_str_beijing():
    return beijing_now().strftime("%Y%m%d")


def in_trade_window(now_beijing=None):
    now = now_beijing or beijing_now()
    if now.weekday() >= 5:          # 周六、周日
        return False
    t = (now.hour, now.minute)
    return TRADING_START <= t <= TRADING_END


def prefix_of(code):
    c = code[0] if code else "6"
    if c in ("0", "3"):
        return "sz"
    if c in ("6", "9"):
        return "sh"
    if c in ("8", "4"):
        return "bj"
    return "sh"


def get_spot_map(codes):
    """批量拉取实时行情。返回 {6位代码: {...}}。字段含 name/price/pct/time/high_limit。"""
    if not codes:
        return {}
    q = ",".join(prefix_of(c) + c for c in codes)
    url = SPOT_URL + q
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    raw = urllib.request.urlopen(req, timeout=15).read().decode("gbk", "ignore")
    result = {}
    for line in raw.split(";"):
        line = line.strip()
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        val = val.strip('"')
        parts = val.split("~")
        if len(parts) < 50:
            continue
        code = re.sub(r"\D", "", key[2:] if key.startswith("v_") else key).zfill(6)
        try:
            price = float(parts[3])
            high_limit = float(parts[47]) if parts[47] else 0.0
            pct = float(parts[32]) if len(parts) > 32 and parts[32] else 0.0
        except (ValueError, TypeError):
            price = high_limit = pct = 0.0
        result[code] = {
            "name": parts[1], "price": price, "pct": pct,
            "time": parts[30], "high_limit": high_limit,
        }
    return result


def load_pool():
    """读取 all_sectors_pool.csv -> [(sector, code, name), ...]"""
    pool = []
    with open(POOL_CSV, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            sec = (row.get("sector") or "").strip()
            code = re.sub(r"\D", "", str(row.get("code", ""))).zfill(6)
            name = (row.get("name") or "").strip()
            if sec and code:
                pool.append((sec, code, name))
    return pool


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


def detect_stock_breakouts(pool, spot):
    """返回 [(sector, code, name, pct, is_limit_up), ...] 当天所有突破个股（含涨停）。"""
    out = []
    for sector, code, name in pool:
        v = spot.get(code)
        if not v:
            continue
        pct = v.get("pct") or 0.0
        price = v.get("price") or 0.0
        hl = v.get("high_limit") or 0.0
        is_limit_up = (hl > 0 and price >= hl * 0.995)
        if pct >= STOCK_BREAKOUT_PCT or is_limit_up:
            out.append((sector, code, name, pct, is_limit_up))
    return out


# ======================================================================
# 状态（跨 cron 去重，每日重置）
# ======================================================================
def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"date": "", "sector_pushed": [], "stock_pushed": []}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ======================================================================
# 单次检查（核心）
# ======================================================================
def check_once(push=True, spot=None, pool=None):
    date = today_str_beijing()
    state = load_state()
    if state.get("date") != date:
        state = {"date": date, "sector_pushed": [], "stock_pushed": []}

    if pool is None:
        pool = load_pool()
    codes = [c for _, c, _ in pool]
    if spot is None:
        spot = get_spot_map(codes)
    if not spot:
        return [], []

    # 校验行情日期为今日（北京时间）：避免盘前/隔夜用上一交易日数据误判突破
    sample = next(iter(spot.values()))
    quote_date = (sample.get("time") or "")[:8]
    if quote_date and quote_date != date:
        return [], []

    # ---- 板块突破 ----
    avgs = compute_sector_avgs(pool, spot)
    new_sectors = []
    for s, (avg, n, ranked) in avgs.items():
        if avg >= SECTOR_BREAKOUT_PCT and s not in state["sector_pushed"]:
            new_sectors.append((s, avg, n, ranked))
            state["sector_pushed"].append(s)

    # ---- 个股突破 ----
    stocks = detect_stock_breakouts(pool, spot)
    new_stocks = []
    for (sector, code, name, pct, is_lu) in stocks:
        if code not in state["stock_pushed"]:
            new_stocks.append((sector, code, name, pct, is_lu))
            state["stock_pushed"].append(code)

    if new_sectors or new_stocks:
        if push:
            title, desp = build_message(new_sectors, new_stocks, date)
            pu.push_serverchan(title, desp)
            print("→ 已推送突破预警")
        save_state(state)
    else:
        # 即便无新突破也保存（date 字段需要保持当天，便于次日重置判断）
        save_state(state)
    return new_sectors, new_stocks


def build_message(new_sectors, new_stocks, date):
    ts = beijing_now().strftime("%H:%M")
    lines = [f"检测时间：{date} {ts}（北京时间）", ""]
    if new_sectors:
        lines.append("【📈 板块突破 +1%】")
        for (s, avg, n, ranked) in new_sectors:
            top = "、".join(f"{nm}+{p:.1f}%" for nm, p in ranked[:3])
            lines.append(f"• {s}：板块平均 +{avg:.2f}%（{n} 只样本）领涨 {top}")
        lines.append("")
    if new_stocks:
        lines.append("【🚀 个股突破 +3% / 涨停】")
        for (sector, code, name, pct, is_lu) in new_stocks:
            tag = " 涨停🔥" if is_lu else ""
            lines.append(f"• {name}({code}) [{sector}] +{pct:.2f}%{tag}")
        lines.append("")
    lines.append("（同一板块/个股当天仅首次突破推送一次）")
    title = (f"🚨突破预警 {date}｜板块{len(new_sectors)} 个股{len(new_stocks)}")
    return title, "\n".join(lines)


# ======================================================================
# 命令入口
# ======================================================================
def test_push():
    ok = pu.push_serverchan(
        "✅突破监控系统已上线",
        "板块+1% 突破 与 个股+3% 突破 双路监控已接入 GitHub Actions（每5分钟巡检）。\n"
        "交易时段内一旦触发，将立即推送本条同款告警到你的微信。\n"
        "本通知为部署自检，非真实突破。")
    print("test_push 结果:", ok)
    return ok


def selftest():
    print(">>> 突破监控逻辑自检（mock 数据，不联网）")
    pool = [
        ("共封装光学", "300308", "中际旭创"),
        ("共封装光学", "300570", "太辰光"),
        ("创新药", "600276", "恒瑞医药"),
        ("存储芯片", "002049", "紫光国微"),
    ]
    date = today_str_beijing()
    spot = {
        "300308": {"name": "中际旭创", "price": 100, "pct": 6.1, "time": date + "100000", "high_limit": 110},
        "300570": {"name": "太辰光",   "price": 50,  "pct": 5.2, "time": date + "100000", "high_limit": 55},
        "600276": {"name": "恒瑞医药", "price": 40,  "pct": -1.0, "time": date + "100000", "high_limit": 44},
        "002049": {"name": "紫光国微", "price": 80,  "pct": 1.5, "time": date + "100000", "high_limit": 88},
    }
    # 板块平均：共封装光学 (6.1+5.2)/2=5.65% 突破；创新药 -1%；存储芯片 1.5% 突破
    avgs = compute_sector_avgs(pool, spot)
    assert avgs["共封装光学"][0] > 1.0
    assert avgs["存储芯片"][0] > 1.0
    assert avgs["创新药"][0] < 1.0
    stocks = detect_stock_breakouts(pool, spot)
    codes = {s[1] for s in stocks}
    # 300308(+6.1%)、300570(+5.2%) 触发个股突破；002049(+1.5%)仅为板块突破、不入个股；600276(-1%)无
    assert "300308" in codes and "300570" in codes
    assert "002049" not in codes and "600276" not in codes
    print("  板块/个股突破检测通过 ✅")

    # 涨停判定
    spot_lu = {"300308": {"name": "中际旭创", "price": 110, "pct": 20.0, "time": date + "100000", "high_limit": 110}}
    lu = detect_stock_breakouts(pool, spot_lu)
    assert lu and lu[0][4] is True, "涨停应被识别"
    print("  涨停判定通过 ✅")

    # 去重：第二次调用不应再报
    st = {"date": date, "sector_pushed": [], "stock_pushed": []}
    save_state(st)
    ns, nk = check_once(push=False, spot=spot, pool=pool)
    assert len(ns) == 2 and len(nk) == 2  # 板块:共封装光学+存储芯片；个股:300308+300570
    ns2, nk2 = check_once(push=False, spot=spot, pool=pool)  # 第二次，应全去重
    assert len(ns2) == 0 and len(nk2) == 0, "第二次调用应全部去重"
    print("  当日去重通过 ✅")
    print(">>> 自检全部通过 ✅")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    elif "--test-push" in sys.argv:
        test_push()
    elif "--once" in sys.argv:
        if not in_trade_window():
            print("[%s] 非交易时段（北京时间），本次不扫描，静默退出。" %
                  beijing_now().strftime("%Y-%m-%d %H:%M"))
            sys.exit(0)
        ns, nk = check_once(push=True)
        print(f"本轮：板块新增突破 {len(ns)}，个股新增突破 {len(nk)}")
    else:
        print("用法: --once | --test-push | --selftest")
