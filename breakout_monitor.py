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
  C. 市场情绪：板块均值 + 4个股 涨跌幅绝对值首次跨越 ±0.1% → 边缘触发推送（交易时段内可多次）
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
# 状态文件路径（可被 --state 覆盖，便于双工作流各持独立状态，避免 git 提交冲突）
STATE_PATH = STATE_FILE

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
    "市场情绪":   {"mode": "bidirectional", "threshold": 0.1, "dedup": False,
                  "note": "板块均值 + 4个股 涨跌幅绝对值 > 0.1% 即边缘触发推送，交易时段内可多次"},
}

# 市场情绪特殊代码（同花顺概念指数 / 新加坡 A50 期指）需要单独数据源
SPECIAL_CODE_PATTERNS = {
    "ths_concept": re.compile(r"^883\d{3}$|^880\d{3}$"),   # 同花顺概念/行业指数
    "a50_futures": re.compile(r"^CN0Y$|^CN00Y$", re.I),   # 富时 A50 期指连续
}

# 市场情绪重复提醒最小间隔（秒）：边缘触发已取代旧逻辑，此常量保留兼容
SENTIMENT_COOLDOWN_SECONDS = 0


# ======================================================================
# 工具
# ======================================================================
def http_get(url, headers=None, retries=3, timeout=15):
    """带重试的标准库 GET。同花顺/东财接口偶发 502/超时，重试可大幅提升稳定性。"""
    last_err = None
    for _ in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers or {"User-Agent": "Mozilla/5.0"})
            return urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8", "ignore")
        except Exception as e:
            last_err = e
            continue
    if last_err:
        print(f"[http_get] 失败(重试{retries}次): {url[:80]} -> {last_err}")
    return None


def http_get_json(url, headers=None, retries=3, timeout=15):
    raw = http_get(url, headers=headers, retries=retries, timeout=timeout)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None

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
        raw = http_get(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://q.10jqka.com.cn/"
        }, retries=4, timeout=15)
        if not raw:
            return {}
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
        d = http_get_json(url, headers={"User-Agent": "Mozilla/5.0"}, retries=4, timeout=15)
        if not d:
            return {}
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
    # 1) 普通 A 股走腾讯接口（失败不致命：不能因为腾讯抖动就中断情绪监控）
    if normal:
        q = ",".join(prefix_of(c) + c for c in normal)
        url = SPOT_URL + q
        try:
            raw = http_get(url, retries=3, timeout=15)
            if raw:
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
        except Exception as e:
            print(f"[get_spot_map] 腾讯接口异常(普通A股)，仅情绪监控继续: {e}")
    # 2) 特殊代码（同花顺概念指数 / A50 期指）：独立源，优先保证情绪可用
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


def detect_sentiment_alerts(pool, spot, state, threshold=None):
    """
    市场情绪板块：双向阈值(±threshold) 边缘触发告警。
      - 对 4 个个股/指标分别做「区间内(|pct|<=阈值) → 区间外(|pct|>阈值)」跨越检测，跨越才推送；
      - 额外计算 4 指标涨跌幅的「板块均值」，均值跨越 ±threshold 也推送。
    边缘触发可在 ±0.1% 窄阈值下避免持续刷屏，同时保留交易时段内多次触发能力（"无限次"）。
    状态存于 state['sentiment_region']：code / '__avg__' -> 'in'/'out'。
    返回 [(sector, code, name, pct, direction, is_avg), ...]
    """
    out = []
    rule = SECTOR_RULES.get("市场情绪", {})
    if rule.get("mode") != "bidirectional":
        return out
    if threshold is None:
        threshold = float(rule.get("threshold", 0.1))
    region = state.setdefault("sentiment_region", {})

    sent = []
    for sector, code, name in pool:
        if sector != "市场情绪":
            continue
        v = spot.get(code)
        if not v:
            continue
        sent.append((code, name, v.get("pct") or 0.0))

    # 个股/指标 边缘触发
    for code, name, pct in sent:
        r = "out" if abs(pct) >= threshold else "in"
        prev = region.get(code, "in")
        if prev == "in" and r == "out":
            direction = "rise" if pct > 0 else "fall"
            out.append(("市场情绪", code, name, pct, direction, False))
        region[code] = r

    # 板块均值 边缘触发
    if sent:
        avg = sum(p for _, _, p in sent) / len(sent)
        r = "out" if abs(avg) >= threshold else "in"
        prev = region.get("__avg__", "in")
        if prev == "in" and r == "out":
            direction = "rise" if avg > 0 else "fall"
            out.append(("市场情绪", "__avg__", "市场情绪(均值)", avg, direction, True))
        region["__avg__"] = r
    return out


# ======================================================================
# 状态（跨 cron 去重，每日重置）
# ======================================================================
def load_state():
    try:
        with open(STATE_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"date": "", "sector_pushed": [], "stock_pushed": [],
                "sentiment_last_alert": {}, "sentiment_region": {}}


def save_state(state):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ======================================================================
# 单次检查（核心）
# ======================================================================
def check_once(push=True, spot=None, pool=None):
    date = today_str_beijing()
    now = beijing_now()
    state = load_state()
    if state.get("date") != date:
        state = {"date": date, "sector_pushed": [], "stock_pushed": [],
                 "sentiment_last_alert": {}, "sentiment_region": {}}

    if pool is None:
        pool = load_pool()
    codes = [c for _, c, _ in pool]
    if spot is None:
        spot = get_spot_map(codes)
    if not spot:
        return [], [], []

    # 行情日期校验（软化）：只要「任一」有效行情日期是今天就放行，
    # 避免单一股票时间字段异常（或腾讯偶发空数据）误杀整体，也保证情绪源(同花顺/东财)可用时必跑。
    valid_dates = {(v.get("time") or "")[:8] for v in spot.values() if v.get("time")}
    if valid_dates and date not in valid_dates:
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

    # ---- 市场情绪双向告警（边缘触发，±0.1%）----
    sentiment_rule = SECTOR_RULES.get("市场情绪", {})
    sentiment_threshold = float(sentiment_rule.get("threshold", 0.1))
    new_sentiment = detect_sentiment_alerts(sentiment_pool, spot, state, sentiment_threshold)

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
        lines.append("【💓 市场情绪 | 涨跌超 ±0.1%】")
        for (sector, code, name, pct, direction, is_avg) in new_sentiment:
            emoji = "📈" if direction == "rise" else "📉"
            if is_avg:
                lines.append(f"• {emoji} {name}（4指标均值）{pct:+.2f}%")
            else:
                lines.append(f"• {emoji} {name}({code}) {pct:+.2f}%")
        lines.append("")
    if new_sentiment and not new_sectors and not new_stocks:
        lines.append("（市场情绪：板块均值与4个股涨跌幅越±0.1%即触发，交易时段内可多次）")
    else:
        lines.append("（板块/个股当天仅首次突破推送一次；市场情绪越阈值即触发）")
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

    # 市场情绪 ±0.1% 边缘触发
    sent_pool = [
        ("市场情绪", "883993", "昨日非ST首板"),
        ("市场情绪", "883988", "昨日非ST连板"),
        ("市场情绪", "CN0Y", "富时A50期指"),
    ]
    # 三指标都越界：883993 +2%、883988 -2%、CN0Y +0.5%；板块均值 (2-2+0.5)/3=+0.17% 也越界
    sent_spot = {
        "883993": {"name": "昨日非ST首板", "price": 1000, "pct": 2.0, "time": date + "100000", "high_limit": 0},
        "883988": {"name": "昨日非ST连板", "price": 1000, "pct": -2.0, "time": date + "100000", "high_limit": 0},
        "CN0Y":   {"name": "富时A50期指",  "price": 15000, "pct": 0.5, "time": date + "100000", "high_limit": 0},
    }
    save_state({"date": date, "sector_pushed": [], "stock_pushed": [], "sentiment_last_alert": {}, "sentiment_region": {}})
    ns3, nk3, nm3 = check_once(push=False, spot=sent_spot, pool=sent_pool)
    assert len(nm3) == 4, f"市场情绪首次应触发 4 条(3个股+1均值)，实际 {len(nm3)}"
    assert any(c == "883993" and d == "rise" for _, c, _, _, d, _ in nm3)
    assert any(c == "883988" and d == "fall" for _, c, _, _, d, _ in nm3)
    assert any(c == "__avg__" for _, c, _, _, _, _ in nm3), "应包含板块均值触发"
    # 第二次同值扫描：已在区间外，不应重复触发（边缘触发去重）
    ns4, nk4, nm4 = check_once(push=False, spot=sent_spot, pool=sent_pool)
    assert len(nm4) == 0, f"第二次同值不应重复触发，实际 {len(nm4)}"
    # 回落区间内再越界 -> 再次触发（模拟盘中多次穿越）
    sent_spot_in = {k: dict(v, pct=0.0) for k, v in sent_spot.items()}
    check_once(push=False, spot=sent_spot_in, pool=sent_pool)   # 回到 inside
    ns6, nk6, nm6 = check_once(push=False, spot=sent_spot, pool=sent_pool)  # 再越界
    assert len(nm6) == 4, f"回落后再越界应再次触发 4 条，实际 {len(nm6)}"
    print("  市场情绪 ±0.1% 边缘触发检测通过 ✅")
    print(">>> 自检全部通过 ✅")


if __name__ == "__main__":
    if "--state" in sys.argv:
        try:
            i = sys.argv.index("--state")
            if i + 1 < len(sys.argv):
                STATE_PATH = os.path.abspath(sys.argv[i + 1])
        except Exception:
            pass
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
        print("用法: --once [--state 路径] | --test-push | --selftest")
