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
from pathlib import Path

import push_utils as pu

BASE = os.path.dirname(os.path.abspath(__file__))
POOL_CSV = os.path.join(BASE, "all_sectors_pool.csv")
STATE_FILE = os.path.join(BASE, "breakout_state.json")

SECTOR_BREAKOUT_PCT = 1.0   # 板块平均涨幅突破阈值（+1%）
STOCK_BREAKOUT_PCT = 3.0    # 个股涨幅突破阈值（+3%）；另含涨停判定
TRADING_START = (9, 25)
TRADING_END = (15, 0)
SPOT_URL = "https://qt.gtimg.cn/q="

# 板块级告警规则（新增：市场情绪为双向阈值、无限次提醒）
SECTOR_RULES = {
    "共封装光学": {"mode": "breakout", "stock_pct": 3.0, "sector_avg_pct": 1.0},
    "创新药":     {"mode": "breakout", "stock_pct": 3.0, "sector_avg_pct": 1.0},
    "存储芯片":   {"mode": "breakout", "stock_pct": 3.0, "sector_avg_pct": 1.0},
    "稀土永磁":   {"mode": "breakout", "stock_pct": 3.0, "sector_avg_pct": 1.0},
    "市场情绪":   {"mode": "bidirectional", "threshold": 0.01, "dedup": False,
                  "note": "涨幅或跌幅绝对值 > 0.01% 即推送，交易时段内无限次"},
}

# 市场情绪特殊代码（同花顺概念指数 / 新加坡 A50 期指）需要单独数据源
SPECIAL_CODE_PATTERNS = {
    "ths_concept": re.compile(r"^883\d{3}$|^880\d{3}$"),   # 同花顺概念/行业指数
    "a50_futures": re.compile(r"^CN0Y$|^CN00Y$", re.I),   # 富时 A50 期指连续
}

# 市场情绪重复提醒最小间隔（秒）：避免同一分钟内连爆；0 = 完全无间隔
SENTIMENT_COOLDOWN_SECONDS = 0


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


def is_special_code(code):
    """判断是否为腾讯 qt.gtimg.cn 不支持的特殊代码（同花顺概念指数 / 期货）。"""
    return any(p.match(code) for p in SPECIAL_CODE_PATTERNS.values())


def special_code_type(code):
    for typ, pat in SPECIAL_CODE_PATTERNS.items():
        if pat.match(code):
            return typ
    return None


def _parse_jsonp(raw):
    """从 JSONP 形如 quotebridge_xxx({...}) 中提取 JSON 对象。"""
    m = re.search(r'\((\{.*\})\)\s*$', raw.strip())
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def _spot_ths_concept(code):
    """通过 同花顺 K 线接口获取概念指数最新价与涨跌幅（标准库，无第三方依赖）。"""
    url = f"https://d.10jqka.com.cn/v4/line/bk_{code}/01/last.js"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://q.10jqka.com.cn/"
        })
        raw = urllib.request.urlopen(req, timeout=15).read().decode("utf-8", "ignore")
        d = _parse_jsonp(raw)
        if not d:
            return {}
        data_str = d.get("data", "")
        entries = [e.strip() for e in data_str.split(";") if e.strip()]
        if len(entries) < 2:
            return {}
        # 每根 K 线：date,open,high,low,close,volume,amount,...
        today = entries[-1].split(",")
        prev = entries[-2].split(",")
        if len(today) < 5 or len(prev) < 5:
            return {}
        name = d.get("name", "")
        close_today = float(today[4])
        close_prev = float(prev[4])
        pct = (close_today - close_prev) / close_prev * 100 if close_prev else 0.0
        return {
            code: {
                "name": name,
                "price": close_today,
                "pct": pct,
                "time": beijing_now().strftime("%Y%m%d%H%M%S"),
                "high_limit": 0.0,
                "source": "ths_concept",
            }
        }
    except Exception:
        return {}


def _spot_a50_futures(code):
    """通过 Eastmoney 期货接口获取富时 A50 期指实时行情（标准库）。code 可能是 CN0Y/CN00Y。"""
    # 同花顺用 CN0Y，东方财富用 CN00Y
    em_code = "CN00Y" if code.upper() in ("CN0Y", "CN00Y") else code.upper()
    url = f"https://futsseapi.eastmoney.com/static/104_{em_code}_qt"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        raw = urllib.request.urlopen(req, timeout=15).read().decode("utf-8", "ignore")
        d = json.loads(raw)
        qt = d.get("qt", {})
        if not qt:
            return {}
        price = float(qt.get("p", 0) or 0)
        pct = float(qt.get("zdf", 0) or 0)
        name = qt.get("name", f"富时A50期指({code})")
        return {
            code: {
                "name": name,
                "price": price,
                "pct": pct,
                "time": beijing_now().strftime("%Y%m%d%H%M%S"),
                "high_limit": 0.0,
                "source": "a50_futures",
            }
        }
    except Exception:
        return {}


def get_spot_map_special(codes):
    """获取特殊代码（同花顺概念指数 / A50 期指）实时行情。"""
    if not codes:
        return {}
    result = {}
    for c in codes:
        typ = special_code_type(c)
        if typ == "ths_concept":
            result.update(_spot_ths_concept(c))
        elif typ == "a50_futures":
            result.update(_spot_a50_futures(c))
    return result


def get_spot_map(codes):
    """批量拉取实时行情。返回 {代码: {...}}。字段含 name/price/pct/time/high_limit。"""
    if not codes:
        return {}
    normal = [c for c in codes if not is_special_code(c)]
    special = [c for c in codes if is_special_code(c)]
    result = {}
    # 1) 普通 A 股走腾讯接口
    if normal:
        q = ",".join(prefix_of(c) + c for c in normal)
        url = SPOT_URL + q
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        raw = urllib.request.urlopen(req, timeout=15).read().decode("gbk", "ignore")
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
    # 2) 特殊代码走 akshare 备用源
    if special:
        result.update(get_spot_map_special(special))
    return result


def _load_csv_rows(path):
    """安全读取一个 CSV，返回 (sector, code, name) 三元组列表。"""
    rows = []
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        # 自动识别表头：有 sector 列直接用；没有则按文件名推断板块
        has_sector = "sector" in (reader.fieldnames or [])
        inferred_sector = ""
        if not has_sector:
            stem = Path(path).stem
            if stem in SECTOR_RULES:
                inferred_sector = stem
        for row in reader:
            sec = (row.get("sector") or "").strip() if has_sector else inferred_sector
            raw_code = str(row.get("code", "")).strip()
            code = raw_code.upper() if is_special_code(raw_code) else re.sub(r"\D", "", raw_code).zfill(6)
            name = (row.get("name") or "").strip()
            if sec and code:
                rows.append((sec, code, name))
    return rows


def load_pool():
    """读取股票池。
    优先读取 all_sectors_pool.csv；若不存在，自动合并各板块 CSV + 市场情绪.csv。
    普通 A 股保留 6 位数字；特殊代码（CN0Y 等同花顺/期货代码）原样保留。"""
    if os.path.exists(POOL_CSV):
        return _load_csv_rows(POOL_CSV)

    # 降级：动态合并所有以 .csv 结尾的板块文件 + 市场情绪.csv
    pool = []
    sector_files = ["共封装光学.csv", "创新药.csv", "存储芯片.csv", "稀土永磁.csv", "市场情绪.csv"]
    for fname in sector_files:
        path = os.path.join(BASE, fname)
        if os.path.exists(path):
            pool.extend(_load_csv_rows(path))
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


def detect_sentiment_alerts(pool, spot, now_beijing, state):
    """
    市场情绪板块：双向阈值告警，交易时段内无限次。
    返回 [(sector, code, name, pct, direction), ...]，direction='rise'/'fall'。
    用 state['sentiment_last_alert'] 做可选冷却（SENTIMENT_COOLDOWN_SECONDS）。
    """
    out = []
    rule = SECTOR_RULES.get("市场情绪", {})
    if rule.get("mode") != "bidirectional":
        return out
    threshold = float(rule.get("threshold", 0.01))
    last = state.setdefault("sentiment_last_alert", {})   # code_direction -> iso timestamp

    for sector, code, name in pool:
        if sector != "市场情绪":
            continue
        v = spot.get(code)
        if not v:
            continue
        pct = v.get("pct") or 0.0
        direction = None
        if pct > threshold:
            direction = "rise"
        elif pct < -threshold:
            direction = "fall"
        if not direction:
            continue
        key = f"{code}_{direction}"
        last_ts = last.get(key)
        if SENTIMENT_COOLDOWN_SECONDS > 0 and last_ts:
            last_dt = datetime.datetime.fromisoformat(last_ts)
            if (now_beijing - last_dt).total_seconds() < SENTIMENT_COOLDOWN_SECONDS:
                continue
        out.append((sector, code, name, pct, direction))
        last[key] = now_beijing.isoformat()
    return out


# ======================================================================
# 状态（跨 cron 去重，每日重置）
# ======================================================================
def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"date": "", "sector_pushed": [], "stock_pushed": [], "sentiment_last_alert": {}}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ======================================================================
# 单次检查（核心）
# ======================================================================
def check_once(push=True, spot=None, pool=None):
    date = today_str_beijing()
    now = beijing_now()
    state = load_state()
    if state.get("date") != date:
        state = {"date": date, "sector_pushed": [], "stock_pushed": [], "sentiment_last_alert": {}}

    if pool is None:
        pool = load_pool()
    codes = [c for _, c, _ in pool]
    if spot is None:
        spot = get_spot_map(codes)
    if not spot:
        return [], [], []

    # 校验行情日期为今日（北京时间）：避免盘前/隔夜用上一交易日数据误判突破
    # 特殊代码（期货/概念指数）的时间字段可能为空，放行
    sample = next(iter(spot.values()))
    quote_date = (sample.get("time") or "")[:8]
    if quote_date and quote_date != date:
        return [], [], []

    # 按板块规则分组
    breakout_pool = [(s, c, n) for (s, c, n) in pool
                     if SECTOR_RULES.get(s, {}).get("mode", "breakout") == "breakout"]
    sentiment_pool = [(s, c, n) for (s, c, n) in pool
                      if SECTOR_RULES.get(s, {}).get("mode") == "bidirectional"]

    # ---- 板块突破（原逻辑，仅对 breakout 模式板块）----
    new_sectors = []
    if breakout_pool:
        avgs = compute_sector_avgs(breakout_pool, spot)
        for s, (avg, n, ranked) in avgs.items():
            rule = SECTOR_RULES.get(s, {})
            threshold = float(rule.get("sector_avg_pct", SECTOR_BREAKOUT_PCT))
            if avg >= threshold and s not in state["sector_pushed"]:
                new_sectors.append((s, avg, n, ranked))
                state["sector_pushed"].append(s)

    # ---- 个股突破（原逻辑，仅对 breakout 模式板块）----
    new_stocks = []
    if breakout_pool:
        stocks = detect_stock_breakouts(breakout_pool, spot)
        for (sector, code, name, pct, is_lu) in stocks:
            if code not in state["stock_pushed"]:
                new_stocks.append((sector, code, name, pct, is_lu))
                state["stock_pushed"].append(code)

    # ---- 市场情绪双向告警（无限次）----
    new_sentiment = detect_sentiment_alerts(sentiment_pool, spot, now, state)

    if new_sectors or new_stocks or new_sentiment:
        if push:
            title, desp = build_message(new_sectors, new_stocks, new_sentiment, date)
            pu.push_serverchan(title, desp)
            print("→ 已推送突破预警")
        save_state(state)
    else:
        # 即便无新突破也保存（date 字段需要保持当天，便于次日重置判断）
        save_state(state)
    return new_sectors, new_stocks, new_sentiment


def build_message(new_sectors, new_stocks, new_sentiment, date):
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
    if new_sentiment:
        lines.append("【💓 市场情绪 | 涨跌超 0.01%】")
        for (sector, code, name, pct, direction) in new_sentiment:
            emoji = "📈" if direction == "rise" else "📉"
            lines.append(f"• {emoji} {name}({code}) {pct:+.2f}%")
        lines.append("")
    if new_sentiment and not new_sectors and not new_stocks:
        lines.append("（市场情绪指标在交易时段内持续监控，无限次提醒）")
    else:
        lines.append("（板块/个股当天仅首次突破推送一次；市场情绪无限次）")
    title = (f"🚨突破预警 {date}｜板块{len(new_sectors)} 个股{len(new_stocks)} 情绪{len(new_sentiment)}")
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
    st = {"date": date, "sector_pushed": [], "stock_pushed": [], "sentiment_last_alert": {}}
    save_state(st)
    ns, nk, nm = check_once(push=False, spot=spot, pool=pool)
    assert len(ns) == 2 and len(nk) == 2 and len(nm) == 0  # 板块:共封装光学+存储芯片；个股:300308+300570
    ns2, nk2, nm2 = check_once(push=False, spot=spot, pool=pool)  # 第二次，应全去重
    assert len(ns2) == 0 and len(nk2) == 0 and len(nm2) == 0, "第二次调用应全部去重"
    print("  当日去重通过 ✅")

    # 市场情绪双向阈值 + 无限次
    sent_pool = [
        ("市场情绪", "883993", "昨日非ST首板"),
        ("市场情绪", "883988", "昨日非ST连板"),
        ("市场情绪", "CN0Y", "富时A50期指"),
    ]
    sent_spot = {
        "883993": {"name": "昨日非ST首板", "price": 1000, "pct": 0.02, "time": date + "100000", "high_limit": 0},
        "883988": {"name": "昨日非ST连板", "price": 1000, "pct": -0.02, "time": date + "100000", "high_limit": 0},
        "CN0Y":   {"name": "富时A50期指",  "price": 15000, "pct": 0.005, "time": date + "100000", "high_limit": 0},
    }
    save_state({"date": date, "sector_pushed": [], "stock_pushed": [], "sentiment_last_alert": {}})
    ns3, nk3, nm3 = check_once(push=False, spot=sent_spot, pool=sent_pool)
    assert len(nm3) == 2, "市场情绪应触发 2 条（涨/跌各一）"
    assert any(c == "883993" and d == "rise" for _, c, _, _, d in nm3)
    assert any(c == "883988" and d == "fall" for _, c, _, _, d in nm3)
    # 同方向、同数值再次扫描仍应触发（无限次，仅受可选冷却限制）
    ns4, nk4, nm4 = check_once(push=False, spot=sent_spot, pool=sent_pool)
    assert len(nm4) == 2, "市场情绪应无限次触发"
    print("  市场情绪双向/无限次检测通过 ✅")
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
        ns, nk, nm = check_once(push=True)
        print(f"本轮：板块新增突破 {len(ns)}，个股新增突破 {len(nk)}，市场情绪 {len(nm)}")
    else:
        print("用法: --once | --test-push | --selftest")
