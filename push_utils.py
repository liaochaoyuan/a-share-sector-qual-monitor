#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
共享推送模块：多通道报警
======================================================================
主通道：Server酱（微信服务号，免费秒到）
兜底通道：短信 / 电话（任意提供 HTTP 接口的短信或语音通知服务）

凭据优先级（只看一次，启动时加载）：
  1) 同目录 push_config.json   （推荐，不进版本库）
  2) 环境变量
  3) 本文件默认值（占位，等于不发）

零第三方依赖（仅标准库 urllib），方便部署到轻量云 / 云函数。
======================================================================
"""
import os
import json
import urllib.request
import urllib.parse

BASE = os.path.dirname(os.path.abspath(__file__))

# ========== 默认配置（可被 push_config.json / 环境变量覆盖） ==========
SENDKEY = "你的Server酱SENDKEY"
API = "sctapi"
SMS_VOICE_WEBHOOK = ""
SMS_VOICE_METHOD = "POST"
SMS_VOICE_HEADERS = ""
SMS_VOICE_BODY_TMPL = '{"text":"{title}\\n{desp}"}'


def _load_config():
    global SENDKEY, API, SMS_VOICE_WEBHOOK, SMS_VOICE_METHOD, SMS_VOICE_HEADERS, SMS_VOICE_BODY_TMPL
    # 1) push_config.json（同目录，推荐）
    cfg = os.path.join(BASE, "push_config.json")
    if os.path.exists(cfg):
        try:
            with open(cfg, "r", encoding="utf-8") as f:
                d = json.load(f)
            SENDKEY = d.get("serverchan_sendkey", SENDKEY)
            API = d.get("serverchan_api", API)
            SMS_VOICE_WEBHOOK = d.get("sms_voice_webhook", SMS_VOICE_WEBHOOK)
            SMS_VOICE_METHOD = d.get("sms_voice_method", SMS_VOICE_METHOD)
            SMS_VOICE_HEADERS = d.get("sms_voice_headers", SMS_VOICE_HEADERS)
            SMS_VOICE_BODY_TMPL = d.get("sms_voice_body_tmpl", SMS_VOICE_BODY_TMPL)
        except Exception:
            pass
    # 2) 环境变量（最高优先，便于云函数 / 容器注入）
    SENDKEY = os.environ.get("SERVERCHAN_SENDKEY", SENDKEY)
    API = os.environ.get("SERVERCHAN_API", API)
    SMS_VOICE_WEBHOOK = os.environ.get("SMS_VOICE_WEBHOOK", SMS_VOICE_WEBHOOK)
    SMS_VOICE_METHOD = os.environ.get("SMS_VOICE_METHOD", SMS_VOICE_METHOD)
    SMS_VOICE_HEADERS = os.environ.get("SMS_VOICE_HEADERS", SMS_VOICE_HEADERS)
    SMS_VOICE_BODY_TMPL = os.environ.get("SMS_VOICE_BODY_TMPL", SMS_VOICE_BODY_TMPL)


_load_config()


def push_serverchan(title, desp):
    """推送到微信（Server酱）。未配置 Key 时仅返回 skipped。标准库实现，无第三方依赖。"""
    if SENDKEY in ("", "你的Server酱SENDKEY"):
        return {"skipped": True, "reason": "no_serverchan_key"}
    host = "sc.ftqq.com" if API == "sc" else "sctapi.ftqq.com"
    url = f"https://{host}/{SENDKEY}.send"
    data = urllib.parse.urlencode({"title": title, "desp": desp}).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        resp = urllib.request.urlopen(req, timeout=10).read().decode("utf-8", "ignore")
        return {"ok": True, "resp": resp[:200]}
    except Exception as e:
        return {"error": str(e)}


def push_sms_voice(title, desp):
    """兜底：短信 / 电话。未配置 Webhook 时返回 skipped。"""
    if not SMS_VOICE_WEBHOOK:
        return {"skipped": True, "reason": "no_sms_webhook"}
    try:
        body = SMS_VOICE_BODY_TMPL.format(title=title, desp=desp).encode("utf-8")
        req = urllib.request.Request(SMS_VOICE_WEBHOOK, data=body, method=SMS_VOICE_METHOD)
        req.add_header("Content-Type", "application/json")
        for k, v in (json.loads(SMS_VOICE_HEADERS) if SMS_VOICE_HEADERS else {}).items():
            req.add_header(k, v)
        resp = urllib.request.urlopen(req, timeout=10).read().decode("utf-8", "ignore")
        return {"ok": True, "resp": resp[:200]}
    except Exception as e:
        return {"error": str(e)}


def push_all(title, desp):
    """主通道 + 兜底通道一起发。返回各通道结果。"""
    return {"serverchan": push_serverchan(title, desp),
            "sms_voice": push_sms_voice(title, desp)}
