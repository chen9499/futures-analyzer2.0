# ==================== Kimi大模型分析 v5.0 ====================
"""
llm_analyzer.py v5.0
融入三大新维度：
  - 多周期共振分析
  - 板块轮动预测
  - 风险评分与头寸管理
输出完整的"操作等级 + 头寸建议 + 止损止盈"
"""

import requests
import time
from datetime import datetime
from typing import Optional, Dict, Any

# ─── LLM 配置 ─────────────────────────────────────────────────
_api_key: Optional[str] = None
_base_url: Optional[str] = None
_model: Optional[str] = None


def _ensure_config():
    global _api_key, _base_url, _model
    if _api_key is None:
        from config import KIMI_API_KEY, KIMI_BASE_URL, MODEL_NAME
        _api_key = KIMI_API_KEY
        _base_url = KIMI_BASE_URL
        _model = MODEL_NAME


# ================================================================
#  Kimi API 调用
# ================================================================

def call_kimi(prompt: str) -> str:
    """调用Kimi大模型（无system prompt，避免过滤）"""
    _ensure_config()

    if _api_key == "你的Kimi API Key" or not _api_key:
        return "[Kimi API未配置]"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_api_key}"
    }

    system_hint = (
        "你是专业的期货品种数据分析助手，用简洁中文回复。\n"
        "分析风格：注重多维度共振，控制风险，给出明确操作建议。\n"
        "回复要求：做多/做空各不超过3个，每个逻辑不超过20字，"
        "总体策略不超过50字，风险提示不超过20字。\n"
        "必须输出：推荐等级(强烈推荐/中性偏多/中性/中性偏空/谨慎) + "
        "操作等级(1=强做多/2=轻做多/0=观望/-2=轻做空/-1=强做空) + "
        "头寸手数 + 止损止盈位。"
    )

    messages = [
        {"role": "system", "content": system_hint},
        {"role": "user", "content": prompt}
    ]

    try:
        response = requests.post(
            f"{_base_url}/chat/completions",
            headers=headers,
            json={"model": _model, "messages": messages, "temperature": 0.3},
            timeout=90
        )
        response.raise_for_status()
        result = response.json()
        return result["choices"][0]["message"]["content"]
    except requests.exceptions.Timeout:
        return "请求超时"
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            return "API Key无效"
        elif e.response.status_code == 429:
            return "频率超限"
        else:
            return f"请求失败: {e}"
    except Exception as e:
        return f"异常: {str(e)}"


# ================================================================
#  格式化工具函数
# ================================================================

def _fmt_tech(tech: dict) -> str:
    """格式化技术指标"""
    if not tech:
        return "无数据"
    parts = []
    rsi = tech.get("rsi", 0)
    parts.append(f"RSI={rsi:.0f}({'超买' if rsi>70 else '超卖' if rsi<30 else '中性'})")
    hist = tech.get("macd_hist", 0)
    parts.append(f"MACD={'金叉' if hist>0 else '死叉'}({hist:+.2f})")
    bb = tech.get("bb_position", 0.5)
    parts.append(f"布林={'上轨' if bb>0.8 else '下轨' if bb<0.2 else '中轨'}({bb:.0%})")
    if tech.get("golden_cross"):
        parts.append("均线金叉")
    if tech.get("dead_cross"):
        parts.append("均线死叉")
    parts.append(f"量比={tech.get('volume_ratio',1):.1f}x")
    return " | ".join(parts)


def _fmt_mtf(mtf: dict) -> str:
    """格式化多周期共振"""
    if not mtf:
        return "-"
    rl = mtf.get("resonance_level", "")
    rs = mtf.get("resonance_score", 0)
    al = mtf.get("action_level", 0)
    divergences = mtf.get("divergence", [])
    div_str = f" 背离:{divergences[0]['pair']}" if divergences else ""
    al_map = {1: "强做多", 2: "轻做多", 0: "观望", -2: "轻做空", -1: "强做空"}
    return f"{rl}({rs:+.0f}分)[操作:{al_map.get(al,'?')}]{div_str}"


def _fmt_risk(risk: dict) -> str:
    """格式化风险评分"""
    if not risk:
        return "-"
    lvl = risk.get("risk_level", "-")
    score = risk.get("risk_score", 0)
    warnings = risk.get("warnings", [])
    w_str = f" ⚠{warnings[0][:15]}" if warnings else ""
    return f"{lvl}({score:.0f}分){w_str}"


def _fmt_position(pos: dict) -> str:
    """格式化头寸建议"""
    if not pos:
        return "-"
    lots = pos.get("position_lots", 0)
    sl = pos.get("stop_loss", 0)
    tp = pos.get("take_profit", 0)
    rr = pos.get("rr_ratio", 0)
    if lots <= 0:
        return "不建议开仓"
    sl_str = f"{sl:.2f}" if isinstance(sl, float) else str(sl)
    tp_str = f"{tp:.2f}" if isinstance(tp, float) else str(tp)
    return f"{lots}手 止损{sl_str} 止盈{tp_str} 盈亏比{rr}"


def _fmt_sector(sector_data: dict) -> str:
    """格式化板块轮动"""
    if not sector_data or sector_data.get("status") != "ok":
        return "-"
    ranked = sector_data.get("ranked_sectors", [])
    pred = sector_data.get("prediction", {})
    next_hot = pred.get("next_hot_prediction", "-")
    confidence = pred.get("confidence", 0)
    macro_env = pred.get("macro_environment", "-")
    strong = sector_data.get("summary", {}).get("strong_sectors", [])
    weak = sector_data.get("summary", {}).get("weak_sectors", [])
    lines = []
    lines.append(f"宏观环境: {macro_env}")
    if strong:
        lines.append(f"强势板块: {', '.join(strong)}")
    if weak:
        lines.append(f"弱势板块: {', '.join(weak)}")
    if next_hot and next_hot != "-":
        lines.append(f"下一热点: {next_hot}(置信{confidence}%)")
    if ranked:
        strength = sector_data.get("sector_strength", {})
        order = [f"{s}({strength.get(s,{}).get('strength',0):+.0f})" for s in ranked[:4]]
        lines.append(f"强弱排序: {' > '.join(order)}")
    return " | ".join(lines)


# ================================================================
#  构建 v5.0 分析提示词
# ================================================================

def build_analysis_prompt_v5(all_data: dict) -> str:
    """
    构建 v5.0 全维度分析提示词
    新增：多周期共振 + 板块轮动 + 风险评分与头寸管理
    """
    today = datetime.now().strftime("%Y年%m月%d日 %H:%M")

    cat_data       = all_data.get("categories_data", {})
    mtf_data       = all_data.get("mtf_data", {})       # 多周期共振
    sector_data    = all_data.get("sector_data", {})     # 板块轮动
    risk_data      = all_data.get("risk_data", {})      # 风险评分
    inventory_trend = all_data.get("inventory_trend", {})
    basis_data     = all_data.get("basis_data", {})
    positions_data = all_data.get("positions_data", {})
    scores_data    = all_data.get("scores_data", {})
    policy_data    = all_data.get("policy_data", {})
    sentiment_data = all_data.get("sentiment_data", {})
    risk_data_geo  = all_data.get("risk_data_geo", all_data.get("risk_data", {}))
    news           = all_data.get("news", [])

    valid = [v for v in cat_data.values() if v.get("status") == "OK"]
    sorted_valid = sorted(valid, key=lambda x: x.get("change_pct", 0), reverse=True)
    rising  = sum(1 for v in valid if v.get("change_pct", 0) > 0)
    falling = sum(1 for v in valid if v.get("change_pct", 0) < 0)
    avg_chg = sum(v.get("change_pct", 0) for v in valid) / len(valid) if valid else 0

    lines = [f"【{today} 国内期货全维度智能早参 v5.0】"]
    lines.append(f"全品种 {len(valid)} 个 | 涨 {rising} | 跌 {falling} | 均 {avg_chg:+.2f}%\n")

    # ══════════════════════════════════════════════════════════════
    #  第一模块：板块轮动（最宏观，先看方向）
    # ══════════════════════════════════════════════════════════════
    if sector_data and sector_data.get("status") == "ok":
        sector_str = _fmt_sector(sector_data)
        lines.append(f"【板块轮动】{sector_str}\n")

    # ══════════════════════════════════════════════════════════════
    #  第二模块：多周期共振（核心共振信号）
    # ══════════════════════════════════════════════════════════════
    if mtf_data and mtf_data.get("status") == "ok":
        mtf_summary = mtf_data.get("summary", {})
        strong_buy  = mtf_summary.get("strong_buy", [])
        strong_sell = mtf_summary.get("strong_sell", [])

        lines.append("【多周期共振分析】")
        if strong_buy:
            lines.append(f"  ⭐ 强烈看涨共振: {', '.join(strong_buy[:5])}")
        if strong_sell:
            lines.append(f"  🔻 强烈看跌共振: {', '.join(strong_sell[:5])}")

        # 共振排行（前8）
        results = mtf_data.get("results", {})
        ranked_mtf = list(results.items())[:8]
        lines.append("  共振排行(前8):")
        for i, (sym, res) in enumerate(ranked_mtf, 1):
            rl = res.get("resonance", {})
            score = rl.get("resonance_score", 0)
            level = rl.get("resonance_level", "-")
            action_lvl = rl.get("action_level", 0)
            al_map = {1: "强做多", 2: "轻做多", 0: "观望", -2: "轻做空", -1: "强做空"}
            info = cat_data.get(sym, {})
            price = info.get("price", 0)
            chg   = info.get("change_pct", 0)
            divergences = rl.get("divergence", [])
            div_str = f" 背离:{divergences[0]['pair']}" if divergences else ""
            lines.append(f"    {i}. {sym}: {price} ({chg:+.2f}%) {level}({score:+.0f}分)[{al_map.get(action_lvl,'?')}]{div_str}")
        lines.append("")

    # ══════════════════════════════════════════════════════════════
    #  第三模块：信号评分（传统三维评分）
    # ══════════════════════════════════════════════════════════════
    if scores_data and scores_data.get("status") == "ok":
        summary_s = scores_data.get("summary", {})
        lines.append("【信号强度评分】")
        sb = summary_s.get("strong_buy", [])
        ss = summary_s.get("strong_sell", [])
        rs = summary_s.get("resonance_signals", [])
        if sb:
            lines.append(f"  🟢 强烈推荐: {', '.join(sb[:5])}")
        if ss:
            lines.append(f"  🔴 谨慎: {', '.join(ss[:5])}")
        if rs:
            lines.append(f"  ⭐ 三维共振: {', '.join(rs[:3])}")

        scores_all = scores_data.get("scores", {})
        for i, (sym, si) in enumerate(list(scores_all.items())[:6]):
            sc  = si.get("composite_score", 0)
            rec = si.get("recommendation", "-")
            t, f, s = si.get("technical_score", 0), si.get("fundamental_score", 0), si.get("sentiment_score", 0)
            wr = si.get("win_rate")
            wr_str = f" 胜率:{wr:.0%}" if wr else ""
            lines.append(f"    {i+1}. {sym}: {sc}分[{rec}] 技{t}/基{f}/情{s}{wr_str}")
        lines.append("")

    # ══════════════════════════════════════════════════════════════
    #  第四模块：风险评分与头寸管理（v5.0 核心新增）
    # ══════════════════════════════════════════════════════════════
    if risk_data and risk_data.get("status") == "ok":
        portfolio = risk_data.get("portfolio_risk", {})
        risk_lvl  = portfolio.get("risk_level", "-")
        risk_score = portfolio.get("total_risk_score", 0)
        action    = portfolio.get("action", "-")

        lines.append("【风险评分与头寸管理 v5.0】")
        lines.append(f"  组合风险: {risk_lvl}({risk_score:.0f}分) | {action}")

        # 各品种风险+头寸
        pos_sizes = risk_data.get("position_sizes", {})
        symbol_risks = portfolio.get("symbol_risks", {})
        if pos_sizes:
            sorted_pos = sorted(pos_sizes.items(), key=lambda x: x[1].get("position_lots", 0), reverse=True)
            lines.append("  头寸建议(前5):")
            for sym, pos in sorted_pos[:5]:
                lots = pos.get("position_lots", 0)
                if lots <= 0:
                    continue
                sl = pos.get("stop_loss", 0)
                tp = pos.get("take_profit", 0)
                rr = pos.get("rr_ratio", 0)
                r = symbol_risks.get(sym, {})
                rlvl = r.get("risk_level", "-")
                rsc = r.get("risk_score", 0)
                sl_str = f"{sl:.2f}" if isinstance(sl, float) else str(sl)
                tp_str = f"{tp:.2f}" if isinstance(tp, float) else str(tp)
                lines.append(f"    {sym}: {lots}手 止损{sl_str} 止盈{tp_str} 盈亏比{rr} | 风险{rlvl}({rsc:.0f}分)")

        # 相关性警告
        corr_warnings = portfolio.get("correlations", {}).get("warnings", [])
        if corr_warnings:
            lines.append("  ⚠️ 相关性警告:")
            for w in corr_warnings[:2]:
                lines.append(f"    {w}")
        lines.append("")

    # ══════════════════════════════════════════════════════════════
    #  第五模块：板块内部强弱 + 重点品种
    # ══════════════════════════════════════════════════════════════
    if sector_data and sector_data.get("status") == "ok":
        sector_strength = sector_data.get("sector_strength", {})
        for sector in sector_data.get("ranked_sectors", [])[:3]:
            strength = sector_strength.get(sector, {})
            intra = strength.get("intra_ranking", [])
            if not intra:
                continue
            lines.append(f"【{sector}内部强弱】")
            for r in intra[:3]:
                sym = r["symbol"]
                info = cat_data.get(sym, {})
                chg = info.get("change_pct", 0)
                tech = info.get("tech", {})
                mtf_res = mtf_data.get("results", {}).get(sym, {}).get("resonance", {}) if mtf_data else {}
                mtf_str = _fmt_mtf(mtf_res)
                lines.append(f"  {sym}: {chg:+.2f}% {tech.get('trend','-')} | {_fmt_tech(tech)}")
                lines.append(f"    多周期: {mtf_str}")
            lines.append("")

    # ══════════════════════════════════════════════════════════════
    #  第六模块：重点品种汇总（综合多维度）
    # ══════════════════════════════════════════════════════════════
    lines.append("【重点品种综合信号】")
    focus = sorted_valid[:5] + sorted_valid[-3:]
    for item in focus[:8]:
        sym  = item.get("product", item.get("symbol", "?"))
        price = item.get("price", 0)
        chg  = item.get("change_pct", 0)
        tech = item.get("tech", {})
        mtf_res = mtf_data.get("results", {}).get(sym, {}).get("resonance", {}) if mtf_data else {}
        pos = pos_sizes.get(sym, {}) if risk_data and risk_data.get("status") == "ok" else {}
        r   = symbol_risks.get(sym, {}) if risk_data and risk_data.get("status") == "ok" else {}
        mtf_str  = _fmt_mtf(mtf_res)
        pos_str  = _fmt_position(pos)
        risk_str = _fmt_risk(r)
        inv = all_data.get("inventory_data", {}).get(sym, {})
        inv_str = f" 库:{inv.get('库存','-')}" if inv else ""
        lines.append(f"{sym}: {price} ({chg:+.2f}%)")
        lines.append(f"  技术: {_fmt_tech(tech)}")
        lines.append(f"  共振: {mtf_str}")
        if pos_str and pos_str != "不建议开仓":
            lines.append(f"  头寸: {pos_str}")
        if risk_str != "-":
            lines.append(f"  风险: {risk_str}{inv_str}")
        lines.append("")

    # ══════════════════════════════════════════════════════════════
    #  第七模块：库存趋势（简要）
    # ══════════════════════════════════════════════════════════════
    inv_signals = inventory_trend.get("signals", [])
    if inv_signals:
        lines.append("【库存趋势】")
        for s in inv_signals[:4]:
            lines.append(f"  {s['product']}: {s.get('trend','-')}({s.get('change_pct',0):+.1f}%) -> {s.get('signal','-')}")
        lines.append("")

    # ══════════════════════════════════════════════════════════════
    #  第八模块：期现价差（简要）
    # ══════════════════════════════════════════════════════════════
    if basis_data and basis_data.get("status") == "ok":
        arb = basis_data.get("arb_opportunities", [])
        if arb:
            lines.append("【期现价差】")
            for op in arb[:3]:
                lines.append(f"  {op['product']}: 基差{op['basis_pct']:+.2f}% [{op['basis_type']}] {op['arb_level']}")
            lines.append("")

    # ══════════════════════════════════════════════════════════════
    #  第九模块：机构持仓（简要）
    # ══════════════════════════════════════════════════════════════
    if positions_data and positions_data.get("status") == "ok":
        mf = positions_data.get("money_flow", {})
        sentiment_str = mf.get("overall_sentiment", "-")
        lines.append(f"【机构情绪】整体: {sentiment_str}")
        bullish = mf.get("bullish_symbols", [])
        if bullish:
            lines.append(f"  主力净多: {', '.join(b['symbol'] for b in bullish[:3])}")
        bearish = mf.get("bearish_symbols", [])
        if bearish:
            lines.append(f"  主力净空: {', '.join(b['symbol'] for b in bearish[:3])}")
        lines.append("")

    # ══════════════════════════════════════════════════════════════
    #  第十模块：宏观政策（简要）
    # ══════════════════════════════════════════════════════════════
    if policy_data.get("货币供应"):
        m = policy_data["货币供应"]
        lines.append(f"【宏观】M2 {m.get('M2同比','-')} | M1 {m.get('M1同比','-')}")
    if policy_data.get("CPI"):
        c = policy_data["CPI"]
        lines.append(f"  CPI {c.get('数值','-')} ({c.get('时间','')})")

    # ══════════════════════════════════════════════════════════════
    #  第十一模块：市场情绪（简要）
    # ══════════════════════════════════════════════════════════════
    if sentiment_data:
        parts = []
        if sentiment_data.get("VIX"):
            v = sentiment_data["VIX"]
            parts.append(f"VIX {v['value']:.1f}({v['level']})")
        if sentiment_data.get("南华商品指数"):
            s = sentiment_data["南华商品指数"]
            parts.append(f"南华 {s['price']}({s['change_pct']:+.2f}%)")
        if sentiment_data.get("美元指数"):
            d = sentiment_data["美元指数"]
            parts.append(f"美元 {d['value']:.2f}")
        if parts:
            lines.append(f"【情绪】{' '.join(parts)}")
    lines.append("")

    # ══════════════════════════════════════════════════════════════
    #  第十二模块：财经新闻（前6条）
    # ══════════════════════════════════════════════════════════════
    if news:
        lines.append("【资讯】")
        for n in news[:6]:
            lines.append(f"  {n['title'][:65]}")
        lines.append("")

    # ══════════════════════════════════════════════════════════════
    #  综合分析指令（v5.0 完整版）
    # ══════════════════════════════════════════════════════════════
    lines.append("""
---
【v5.0 综合分析指令】请综合以下全部维度给出判断：

核心判断框架：
1. 多周期共振（日周月均线方向一致+MACD同向=强信号）：
   - 强共振看涨(action=1) → 优先做多
   - 强共振看跌(action=-1) → 优先做空
   - 无共振(action=0) → 观望或轻仓

2. 板块轮动定位：
   - 当前热点板块 → 顺势做多/做空
   - 预测下一热点 → 提前布局
   - 冷门板块 → 谨慎，避免逆势

3. 风险评分控制：
   - 组合风险极高(>80分) → 大幅减仓或观望
   - 组合风险高(60-80分) → 控制仓位，不超过2%
   - 组合风险中(40-60分) → 正常仓位，3-5%
   - 组合风险低(<40分) → 可适当加仓

4. ATR头寸管理：
   - 根据建议手数开仓，不超过计算得出的最大手数
   - 止损位：ATR × 2（波动率止损）
   - 止盈位：止损空间 × 2（1:2盈亏比）

5. 相关性风险：
   - 高度相关品种（RB+I、J+JM、L+PP）避免重复建仓
   - 利用负相关品种（金融期货 vs 商品）进行对冲

6. 库存+基差共振：
   - 去库+正基差 → 现货需求强，确认做多
   - 增库+负基差 → 现货压力大，确认做空

请给出完整分析（含以下所有字段）：
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
推荐等级：强烈推荐/中性偏多/中性/中性偏空/谨慎

做多机会（前3个，共振优先）：
  品种 | 操作等级(1=强/2=轻) | 头寸(手) | 止损 | 止盈 | 逻辑(≤20字) | 风险等级

做空机会（前3个，共振优先）：
  品种 | 操作等级(-1=强/-2=轻) | 头寸(手) | 止损 | 止盈 | 逻辑(≤20字) | 风险等级

组合整体风险等级：（极低/低/中/高/极高）
总头寸建议：（不超过X手）
总体策略：（50字以内）
风险提示：（20字以内）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️ 仅供参考，不构成投资建议，期货有风险，入市须谨慎""")

    return "\n".join(lines)


# ================================================================
#  主分析函数
# ================================================================

def analyze_with_kimi_v5(all_data: dict) -> str:
    """使用Kimi进行v5.0全维度分析（含多周期共振+板块轮动+风险管理）"""
    prompt = build_analysis_prompt_v5(all_data)
    result = call_kimi(prompt)
    return result


# ─── 向后兼容 ──────────────────────────────────────────────────
def build_analysis_prompt_v4(all_data: dict) -> str:
    """向后兼容：调用v5版本"""
    return build_analysis_prompt_v5(all_data)


def analyze_with_kimi_v4(all_data: dict) -> str:
    """向后兼容：调用v5版本"""
    return analyze_with_kimi_v5(all_data)


def build_analysis_prompt_v2(all_data: dict) -> str:
    return build_analysis_prompt_v5(all_data)


def analyze_with_kimi_v2(all_data: dict) -> str:
    return analyze_with_kimi_v5(all_data)
