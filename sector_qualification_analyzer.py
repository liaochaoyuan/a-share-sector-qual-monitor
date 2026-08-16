#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
板块达标分析器 (A股) —— 6 规则打分 + 计数/断档状态机 + 多通道推送
======================================================================
数据源：
  - 实时行情 / 涨停跌停价 / 行情时间：腾讯 qt.gtimg.cn （标准库 urllib）
  - 5 日均线：腾讯 kline 日线接口（web.ifzq.gtimg.cn）
  - 催化语料 / 舆情 / 政策利空 / 个股公告：由 AI 每日联网检索后写入 qual_input.json
功能：
  - 对 4 个细分板块（共封装光学 / 创新药 / 存储芯片 / 稀土永磁）逐日评估 6 条硬性规则；
  - 实现「首日 6 条全过→达标第1天；存续日只需 2-6；断档清零」状态机；
  - 多通道推送：Server酱微信(主) + 短信/电话(兜底, 见 push_utils)；
  - 支持两种运行模式：
      python sector_qualification_analyzer.py              # 单次分析（每日收盘跑一轮）
      python sector_qualification_analyzer.py --loop       # 盘中实时循环（交易时段每5分钟一轮，仅状态变化推送）
      python sector_qualification_analyzer.py --daily-summary  # 只读收评（收盘推送每日汇总，不改状态）
      python sector_qualification_analyzer.py --autoqual   # 无 qual 文件时定性规则标记待研判
      python sector_qualification_analyzer.py --selftest   # 逻辑自检（不联网）
说明：本工具只做「规则条件判断 + 状态计数」，不做任何行情预测。
======================================================================
"""

import os
import re
import sys
import json
import time
import datetime
import urllib.request

import push_utils as pu

# ======================================================================
# 1) 推送凭据配置（统一在 push_utils.py 中加载）
# ======================================================================
# 凭据优先级：同目录 push_config.json  >  环境变量  >  默认值(占位=不发)
# 只需在 push_config.json 填入 serverchan_sendkey 即可启用微信推送；
# 填 sms_voice_webhook 即启用短信/电话兜底。无需改动本文件。
# 详见 push_config.json.example 与 deploy_cloud.md。

# ---- 盘中实时循环参数 ----
LOOP_INTERVAL = 300          # 循环间隔（秒），默认 5 分钟
LOOP_WINDOW_START = (9, 25)  # 交易时段起点
LOOP_WINDOW_END = (15, 15)   # 交易时段终点（含尾盘，确保收盘前最后一轮生效）

# ---- 路径 ----
BASE = os.path.dirname(os.path.abspath(__file__))
POOL_CSV = os.path.join(BASE, "all_sectors_pool.csv")
STATE_FILE = os.path.join(BASE, "qualification_state.json")
QUAL_FILE = os.path.join(BASE, "qual_input.json")
REPORT_FILE = os.path.join(BASE, "qualification_report.txt")
RUN_LOG = os.path.join(BASE, "qualification_run_log.txt")

SPOT_URL = "https://qt.gtimg.cn/q="
KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/kline/kline?param="


# ======================================================================
# 2) 日志
# ======================================================================
def log_line(path, text):
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    except Exception:
        pass

def run_log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    log_line(RUN_LOG, line)


# ======================================================================
# 3) 行情
# ======================================================================
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
            high = float(parts[34]) if len(parts) > 34 and parts[34] else 0.0
            high_limit = float(parts[47]) if parts[47] else 0.0
            low_limit = float(parts[48]) if parts[48] else 0.0
            pct = float(parts[32]) if len(parts) > 32 and parts[32] else 0.0
        except (ValueError, TypeError):
            price = high = high_limit = low_limit = pct = 0.0
        result[code] = {
            "name": parts[1], "price": price, "time": parts[30], "high": high,
            "high_limit": high_limit, "low_limit": low_limit, "pct": pct,
        }
    return result

def get_ma5(code):
    try:
        url = KLINE_URL + f"{prefix_of(code)}{code},day,,,5"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        data = json.loads(urllib.request.urlopen(req, timeout=15).read().decode("utf-8", "ignore"))
        node = data.get("data", {}).get(prefix_of(code) + code, {})
        rows = node.get("day") or node.get("qfqday") or []
        closes = []
        for r in rows:
            try:
                closes.append(float(r[2]))
            except (ValueError, TypeError, IndexError):
                pass
        if closes:
            return sum(closes) / len(closes)
    except Exception as e:
        run_log(f"  MA5 获取失败 {code}: {e}")
    return None


# ======================================================================
# 4) 板块加载
# ======================================================================
def load_sectors():
    import csv
    secs = {}
    with open(POOL_CSV, "r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            sec = (row.get("sector") or "").strip()
            code = re.sub(r"\D", "", str(row.get("code", ""))).zfill(6)
            name = (row.get("name") or "").strip()
            if sec and code:
                secs.setdefault(sec, []).append((code, name))
    return secs


# ======================================================================
# 5) 定性研判
# ======================================================================
def load_qual(autoqual=False):
    default = {
        "catalyst_valid": None, "catalyst_strength": 0,
        "chain_position": "", "multi_resonance": None, "priority": "",
        "policy_negative": False, "stock_negatives": [],
        "sentiment": "", "expected_days": 0, "note": "待AI联网研判",
    }
    if os.path.exists(QUAL_FILE):
        try:
            with open(QUAL_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            out = {}
            for k, v in raw.items():
                d = dict(default)
                d.update(v)
                out[k] = d
            run_log(f"已载入定性研判文件 {QUAL_FILE}（{len(out)} 个板块）")
            return out
        except Exception as e:
            run_log(f"定性研判文件读取失败: {e}")
    if autoqual:
        run_log("⚠️ 无 qual 文件，定性规则(1/5/6/7)标记为「待AI研判」")
        return {}
    run_log("⚠️ 未找到 qual 文件，规则 1/5/6/7 按「未知/不通过」处理（请用 --autoqual 生成占位）")
    return {}


# ======================================================================
# 6) 状态机
# ======================================================================
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ======================================================================
# 7) 单板块评估
# ======================================================================
def evaluate_sector(sector, stocks, spot, ma5_cache, qual_map, today_str, now):
    codes = [c for c, _ in stocks]
    names = {c: n for c, n in stocks}
    valid = [c for c in codes if spot.get(c) and spot[c]["price"] > 0]

    up = sum(1 for c in valid if spot[c]["price"] >= spot[c]["high_limit"] > 0)
    down = sum(1 for c in valid if spot[c]["low_limit"] > 0 and spot[c]["price"] <= spot[c]["low_limit"])
    ratio = (up / down) if down > 0 else (float("inf") if up > 0 else 0.0)
    rule2 = (ratio > 1) if (up + down) > 0 else False

    above = 0
    ma_cnt = 0
    for c in valid:
        ma = ma5_cache.get(c)
        if ma is None:
            ma = get_ma5(c)
            if ma is not None:
                ma5_cache[c] = ma
        if ma:
            ma_cnt += 1
            if spot[c]["price"] > ma:
                above += 1
    frac = (above / ma_cnt) if ma_cnt else 0.0
    rule3 = frac >= 0.5

    in_hours = datetime.time(*LOOP_WINDOW_START) <= now.time() <= datetime.time(*LOOP_WINDOW_END)
    limit_up_now = [c for c in valid if spot[c]["high"] >= spot[c]["high_limit"] > 0]
    if in_hours:
        rule4 = len(limit_up_now) > 1
        rule4_state = "OK" if rule4 else "FAIL"
    else:
        rule4 = None
        rule4_state = "NA"

    q = qual_map.get(sector, {})
    rule1 = q.get("catalyst_valid")
    rule5_neg = bool(q.get("policy_negative"))
    rule6_neg = q.get("stock_negatives") or []
    sentiment = q.get("sentiment", "")
    strength = q.get("catalyst_strength", 0) or 0

    return {
        "sector": sector, "stocks_total": len(codes), "valid": len(valid),
        "up": up, "down": down, "ratio": ratio, "rule2": rule2,
        "above_ma5": above, "ma_cnt": ma_cnt, "ma_frac": frac, "rule3": rule3,
        "limit_up_now": [names.get(c, c) for c in limit_up_now],
        "rule4": rule4, "rule4_state": rule4_state, "in_hours": in_hours,
        "rule1": rule1, "rule5_negative": rule5_neg, "rule6_negatives": rule6_neg,
        "sentiment": sentiment, "strength": strength, "qual": q,
    }

def day_met_for_start(res):
    if res["rule5_negative"]:
        return False
    return (res["rule1"] is True and res["rule2"] is True and
            res["rule3"] is True and res["rule4"] is True)

def day_met_for_keep(res):
    if res["rule5_negative"]:
        return False
    return (res["rule2"] is True and res["rule3"] is True and res["rule4"] is True)


# ======================================================================
# 8) 报告 + 推送
# ======================================================================
def build_report(results, state, today_str):
    lines = []
    lines.append("=" * 64)
    lines.append(f"板块达标分析日报  {today_str}")
    lines.append(f"生成时间：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 64)
    newly, continued, broken, removed = [], [], [], []
    for res in results:
        sec = res["sector"]
        st = state.get(sec, {})
        day = st.get("day_count", 0)
        sym = lambda b: "✅" if b is True else ("❌" if b is False else "⚠️NA")
        lines.append("")
        lines.append(f"【{sec}】  连续达标天数：{day}  样本股：{res['valid']}/{res['stocks_total']}")
        lines.append(f"  规则1 全新真催化 : {sym(res['rule1'])}  催化强度:{res['strength']}  情绪:{res['sentiment'] or '—'}")
        lines.append(f"  规则2 涨跌停比>1 : {sym(res['rule2'])}  涨停{res['up']}/跌停{res['down']} 比={res['ratio']}")
        lines.append(f"  规则3 站上MA5    : {sym(res['rule3'])}  站上{res['above_ma5']}/{res['ma_cnt']} ({res['ma_frac']*100:.0f}%)")
        lines.append(f"  规则4 9:25-9:35涨停>1 : {res['rule4_state']}  盘中涨停:{','.join(res['limit_up_now']) or '无'}")
        lines.append(f"  规则5 政策利空   : {'❌题材失效' if res['rule5_negative'] else '✅无'}")
        if res["rule6_negatives"]:
            lines.append(f"  规则6 个股官方利空: {', '.join(res['rule6_negatives'])}（已从样本剔除）")
        if res["rule5_negative"]:
            removed.append(sec)
        elif day == 1:
            newly.append(sec)
        elif day >= 2:
            continued.append(sec)
    lines.append("")
    lines.append("-" * 64)
    lines.append(f"🆕 今日新达标(第1天): {', '.join(newly) or '无'}")
    lines.append(f"🔥 连续达标中      : {', '.join(continued) or '无'}")
    lines.append(f"💥 今日断档清零    : {', '.join(broken) or '无'}")
    lines.append(f"🚫 政策利空移出池  : {', '.join(removed) or '无'}")
    lines.append("=" * 64)
    return "\n".join(lines), newly, continued, broken, removed

def detect_transitions(state, prev):
    """对比上一轮状态，返回发生变化需要推送的项。"""
    trans = []
    for sec, st in state.items():
        cur = st.get("day_count", 0)
        removed = st.get("removed", False)
        p = prev.get(sec)
        if removed and not (p and p.get("removed")):
            trans.append((sec, "removed"))
        elif p is None:
            if cur >= 1:
                trans.append((sec, "new1"))
        else:
            pc = p.get("day_count", 0)
            if pc == 0 and cur >= 1:
                trans.append((sec, "new1"))
            elif pc >= 1 and cur == 0:
                trans.append((sec, "break"))
    return trans

def status_token(state):
    return {sec: {"day_count": st.get("day_count", 0), "removed": st.get("removed", False)}
            for sec, st in state.items()}


# ======================================================================
# 9) 单次运行（含状态更新）
# ======================================================================
def run_once(autoqual, prev=None, loop=False):
    sectors = load_sectors()
    qual_map = load_qual(autoqual=autoqual)
    all_codes = [c for v in sectors.values() for c, _ in v]
    try:
        spot = get_spot_map(all_codes)
    except Exception as e:
        run_log(f"行情获取失败: {e}")
        return None, None
    if not spot:
        run_log("行情为空。")
        return None, None

    any_time = next(iter(spot.values())).get("time", "")
    trade_date = any_time[:8] if len(any_time) >= 8 else ""
    today_str = datetime.date.today().strftime("%Y%m%d")
    now = datetime.datetime.now()
    ma5_cache = {}
    results = []

    if trade_date and trade_date != today_str:
        # 非交易日快照：不更新状态、不推送
        for sec, stocks in sectors.items():
            results.append(evaluate_sector(sec, stocks, spot, ma5_cache, qual_map, today_str, now))
        report, *_ = build_report(results, load_state(), today_str)
        report = "【快照模式·非交易日】\n" + report
        with open(REPORT_FILE, "w", encoding="utf-8") as f:
            f.write(report + "\n")
        print(report)
        return results, None

    state = load_state()
    for sec, stocks in sectors.items():
        if state.get(sec, {}).get("removed"):
            run_log(f"{sec} 已因政策利空移出选股池，跳过。")
            continue
        res = evaluate_sector(sec, stocks, spot, ma5_cache, qual_map, today_str, now)
        results.append(res)
        st = state.setdefault(sec, {"day_count": 0, "first_day": "", "last_date": "", "removed": False})
        if st["day_count"] == 0:
            if day_met_for_start(res):
                st["day_count"] = 1
                st["first_day"] = today_str
                st["last_date"] = today_str
                run_log(f"★ {sec} 达标第1天！")
            else:
                run_log(f"{sec} 未达成首日6条全过，维持 0 天。")
        else:
            if day_met_for_keep(res):
                st["day_count"] += 1
                st["last_date"] = today_str
                run_log(f"✓ {sec} 连续达标，天数={st['day_count']}")
            else:
                run_log(f"✗ {sec} 断档，连续计数清零（原 {st['day_count']} 天）。")
                st["day_count"] = 0
                st["first_day"] = ""
        if res["rule5_negative"]:
            st["removed"] = True
            run_log(f"🚫 {sec} 出现一级政策利空，移出选股池。")
    save_state(state)

    report, newly, continued, broken, removed = build_report(results, state, today_str)
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    print(report)

    # 推送决策
    if loop:
        trans = detect_transitions(state, prev if prev is not None else {})
        if trans:
            title = "📊板块达标实时" + today_str + "｜" + " ".join(
                f"{s}:{'新达标' if k=='new1' else '断档' if k=='break' else '移出'}" for s, k in trans)
            pu.push_all(title, report)
            run_log(f"→ 实时推送（{len(trans)} 项状态变化）")
        else:
            run_log("→ 本轮无状态变化，不推送。")
    else:
        if newly or continued or broken or removed:
            title = f"📊板块达标日报 {today_str}｜新{len(newly)} 续{len(continued)} 断{len(broken)} 移{len(removed)}"
            pu.push_all(title, report)
        else:
            run_log("当日无状态变化，未推送。")
    return results, state


# ======================================================================
# 10) 盘中实时循环
# ======================================================================
def sleep_to_next_open():
    now = datetime.datetime.now()
    target = now.replace(hour=LOOP_WINDOW_START[0], minute=LOOP_WINDOW_START[1],
                         second=0, microsecond=0)
    if now >= target:
        target = target + datetime.timedelta(days=1)
    # 跳到下一个周一~周五
    while target.weekday() >= 5:
        target += datetime.timedelta(days=1)
    return max(0, (target - now).total_seconds())

def loop_mode(autoqual):
    run_log(f"进入盘中实时循环（每 {LOOP_INTERVAL}s 一轮，仅状态变化推送；窗口 "
            f"{LOOP_WINDOW_START[0]}:{LOOP_WINDOW_START[1]:02d}-{LOOP_WINDOW_END[0]}:{LOOP_WINDOW_END[1]:02d}）")
    prev = status_token(load_state())
    while True:
        try:
            now = datetime.datetime.now()
            if now.weekday() >= 5 or not (datetime.time(*LOOP_WINDOW_START) <= now.time() <= datetime.time(*LOOP_WINDOW_END)):
                secs = sleep_to_next_open()
                run_log(f"非交易时段，休眠 {int(secs)} 秒至下一开盘 ...")
                time.sleep(secs)
                prev = status_token(load_state())  # 重置基线
                continue
            _, state = run_once(autoqual=autoqual, prev=prev, loop=True)
            if state is not None:
                prev = status_token(state)
        except Exception as e:
            run_log(f"循环异常: {e}")
        time.sleep(LOOP_INTERVAL)


# ======================================================================
# 10.5) 每日收评（只读，不改动状态）
# ======================================================================
def daily_summary(autoqual):
    """收盘后（或任意时刻）生成日报并推送；只读取 state，绝不写入，
    因此与盘中 --loop 实时进程互不干扰、无竞争。"""
    sectors = load_sectors()
    qual_map = load_qual(autoqual=autoqual)
    all_codes = [c for v in sectors.values() for c, _ in v]
    try:
        spot = get_spot_map(all_codes)
    except Exception as e:
        run_log(f"行情获取失败: {e}")
        return None
    if not spot:
        run_log("行情为空。")
        return None

    any_time = next(iter(spot.values())).get("time", "")
    trade_date = any_time[:8] if len(any_time) >= 8 else ""
    today_str = datetime.date.today().strftime("%Y%m%d")
    now = datetime.datetime.now()
    ma5_cache = {}
    results = []

    if trade_date and trade_date != today_str:
        # 非交易日：仅快照，不推送收评
        for sec, stocks in sectors.items():
            results.append(evaluate_sector(sec, stocks, spot, ma5_cache, qual_map, today_str, now))
        report, *_ = build_report(results, load_state(), today_str)
        report = "【快照模式·非交易日收评】\n" + report
        with open(REPORT_FILE, "w", encoding="utf-8") as f:
            f.write(report + "\n")
        print(report)
        return None

    state = load_state()
    for sec, stocks in sectors.items():
        if state.get(sec, {}).get("removed"):
            run_log(f"{sec} 已因政策利空移出选股池，跳过。")
            continue
        results.append(evaluate_sector(sec, stocks, spot, ma5_cache, qual_map, today_str, now))

    report, newly, continued, broken, removed = build_report(results, state, today_str)
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    print(report)

    if newly or continued or broken or removed:
        title = f"📊板块达标每日收评 {today_str}｜新{len(newly)} 续{len(continued)} 断{len(broken)} 移{len(removed)}"
        pu.push_all(title, report)
        run_log("→ 已推送每日收评。")
    else:
        run_log("当日无状态变化，未推送收评。")
    return results


# ======================================================================
# 11) 自检
# ======================================================================
def selftest():
    print(">>> 达标状态机自检（mock 数据）")
    base = {
        "sector": "测试", "stocks_total": 3, "valid": 3,
        "up": 2, "down": 0, "ratio": float("inf"), "rule2": True,
        "above_ma5": 3, "ma_cnt": 3, "ma_frac": 1.0, "rule3": True,
        "limit_up_now": ["A", "B"], "rule4": True, "rule4_state": "OK", "in_hours": True,
        "rule1": True, "rule5_negative": False, "rule6_negatives": [],
        "sentiment": "长期主线", "strength": 80, "qual": {},
    }
    assert day_met_for_start(base) is True and day_met_for_keep(base) is True
    broken = dict(base); broken["rule2"] = False
    assert day_met_for_keep(broken) is False
    neg = dict(base); neg["rule5_negative"] = True
    assert day_met_for_start(neg) is False and day_met_for_keep(neg) is False
    na = dict(base); na["rule4"] = None
    assert day_met_for_start(na) is False and day_met_for_keep(na) is False
    # 状态转移检测
    st0 = {"X": {"day_count": 0, "removed": False}}
    st1 = {"X": {"day_count": 1, "removed": False}}
    assert detect_transitions(st1, st0) == [("X", "new1")]
    st2 = {"X": {"day_count": 0, "removed": False}}
    assert detect_transitions(st2, st1) == [("X", "break")]
    print("  状态机 + 转移检测 全部通过 ✅")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    elif "--loop" in sys.argv:
        loop_mode(autoqual="--autoqual" in sys.argv)
    elif "--daily-summary" in sys.argv:
        daily_summary(autoqual="--autoqual" in sys.argv)
    else:
        run_once(autoqual="--autoqual" in sys.argv)
