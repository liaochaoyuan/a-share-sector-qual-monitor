# A股板块「达标」盯盘（GitHub Actions 免费 7×24 云端版）

对 4 个细分板块（共封装光学 / 创新药 / 存储芯片 / 稀土永磁）逐日评估 6 条硬性规则，
实现「首日 6 条全过→达标第1天；存续日只需 2-6；断档清零」状态机，
**仅在「新达标 / 断档 / 因政策利空移出」状态变化时** 通过 Server酱 推送到微信。

## 架构（零成本、关机也能跑）

```
GitHub Actions (ubuntu-latest, 免费)
  └─ 每 10 分钟触发一次 workflow (cron, UTC)
       ├─ 读腾讯 qt.gtimg.cn 实时行情 + K线 MA5（规则 2/3/4 量化）
       ├─ 读 qual_input.json 定性研判（规则 1/5/6/7，由每日联网检索更新）
       ├─ 跑状态机，仅状态变化 → push_all() 推送微信（Server酱）
       └─ 把 qualification_state.json commit 回仓库（跨 job 持久化连续天数）
```

- **不是真·常驻进程**，而是「每 10 分钟跑一次单次分析」。对板块达标这种分钟级~小时级事件足够。
- 非交易时段 analyzer 自动走 snapshot 模式快速退出，不误推、不误改状态。
- 状态文件靠仓库 commit 在 job 之间传递，所以「连续达标天数」不会因无状态环境而丢失。

## 你需要做的两件事

### 1. 配置 Server酱 SENDKEY（推送通道）
1. 打开 https://sct.ftqq.com/ 用微信扫码登录，复制 `SENDKEY`。
2. 进入本仓库 **Settings → Secrets and variables → Actions → New repository secret**：
   - Name: `SERVERCHAN_SENDKEY`
   - Secret: 粘贴你的 SENDKEY
3. 保存后，下次 workflow 运行即会推送到你的微信。

> 原则上也可用 `gh secret set SERVERCHAN_SENDKEY -b "你的KEY" --repo <你>/<仓库>`。

### 2. 保持仓库活跃
GitHub 会在仓库 **60 天无活动** 后自动禁用 scheduled workflows。
每月手动触发一次（Actions 页面 → Run workflow）即可续命。

## 本地运行（调试用）

```bash
pip install -r requirements.txt   # 本项目零第三方依赖，可省
cp push_config.json.example push_config.json   # 填入 SENDKEY
python sector_qualification_analyzer.py            # 单次分析
python sector_qualification_analyzer.py --loop     # 本地盘中实时循环
python sector_qualification_analyzer.py --selftest # 逻辑自检
```

## 文件说明

| 文件 | 作用 |
|------|------|
| `sector_qualification_analyzer.py` | 核心分析器：6 规则 + 状态机 + 变化检测推送 |
| `push_utils.py` | 多通道推送（Server酱微信主通道 + 短信/电话兜底，纯标准库零依赖） |
| `all_sectors_pool.csv` | 监控的 4 板块 39 只个股池 |
| `qual_input.json` | 定性研判数据（催化/政策利空/个股公告），需每日刷新 |
| `push_config.json.example` | 推送配置模板（真实 `push_config.json` 不入库） |
| `.github/workflows/sector-monitor.yml` | GitHub Actions 部署定义 |

## 已知限制 / 可增强

- **定性研判（`qual_input.json`）目前是静态提交**。要每日自动联网刷新，可接入带 LLM/搜索的 workflow（设 `OPENAI_API_KEY` 等后扩展）。
- GitHub Actions cron 偶尔延迟 15~30 分钟，非交易所级实时；若要秒级实时，改用轻量云常驻 `--loop`。
- 公开仓库免费无限分钟；若设为私有，注意每月 2000 分钟免费额度（每 10 分钟触发约 4320 分钟/月，会超）。

## 板块突破实时盯盘（独立于达标 6 规则）

监控 4 个板块，当某板块**当日平均涨幅突破 +1%** 时**立即**推送到微信（不等 10 分钟轮询）。

- 运行方式：`python sector_breakout_monitor.py`（常驻进程，默认每 30 秒轮询）。
- **为什么不放 GitHub Actions**：Actions 的 cron 最小 5 分钟且有 15~30 分钟延迟，做不到「马上」。本监控必须常驻进程（轻量云 / 本机常开）。
- 判定：仅在交易时段（工作日 09:30-15:00）且行情日期为「今日」时生效，避免隔夜数据误报。
- 去重：当天每板块首次突破只推一次，次日自动重置（状态存 `sector_breakout_state.json`）。
- 推送内容：板块名、平均涨幅、成分股数、触发时间、涨幅前 3 个股。
- 调试：`python sector_breakout_monitor.py --once`（单次检查） / `--selftest`（逻辑自检）。

> 与达标监控分工：达标 6 规则（趋势/持续性）→ GitHub Actions 每 10 分钟；板块突破 +1%（即时强度）→ 常驻进程实时。两者互不冲突。
