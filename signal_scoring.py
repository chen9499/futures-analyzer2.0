# ==================== 信号强度评分系统 v1.0 ====================
"""
signal_scoring.py
多维度共振评分（1-10分）：技术面 + 基本面 + 情绪面
历史胜率统计，综合信号强度
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Optional

# ============================================================
#  评分权重配置
# ============================================================

SCORE_WEIGHTS = {
    "technical": 0.40,    # 技术面权重 40%
    "fundamental": 0.35,  # 基本面权重 35%
    "sentiment": 0.25,    # 情绪面权重 25%
}

# 历史胜率记录文件
HISTORY_FILE = os.path.join(os.path.dirname(__file__), "signal_history.json")

# 信号等级阈值
SIGNAL_LEVELS = {
    (8, 10): "强烈推荐",
    (6, 8):  "中性偏多",
    (4, 6):  "中性",
    (2, 4):  "中性偏空",
    (0, 2):  "谨慎",
}


# ============================================================
#  技术面评分
# ============================================================

def score_technical(tech: dict, price: float, change_pct: float) -> dict:
    """
    技术面评分（0-10分）
    综合RSI、MACD、布林带、均线、量比
    """
    if not tech:
        return {"score": 5.0, "details": {}, "signals": []}

    score = 5.0  # 基准分
    details = {}
    signals = []

    # 1. RSI评分（-2 ~ +2）
    rsi = tech.get("rsi", 50)
    if rsi < 30:
        rsi_score = 2.0
        signals.append(f"RSI超卖({rsi:.0f})")
    elif rsi < 40:
        rsi_score = 1.0
        signals.append(f"RSI偏低({rsi:.0f})")
    elif rsi > 70:
        rsi_score = -2.0
        signals.append(f"RSI超买({rsi:.0f})")
    elif rsi > 60:
        rsi_score = -1.0
        signals.append(f"RSI偏高({rsi:.0f})")
    else:
        rsi_score = 0.0
    details["rsi"] = rsi_score
    score += rsi_score

    # 2. MACD评分（-1.5 ~ +1.5）
    macd_hist = tech.get("macd_hist", 0)
    if macd_hist > 0:
        macd_score = min(1.5, macd_hist / 10)
        signals.append(f"MACD金叉(+{macd_hist:.2f})")
    else:
        macd_score = max(-1.5, macd_hist / 10)
        signals.append(f"MACD死叉({macd_hist:.2f})")
    details["macd"] = round(macd_score, 2)
    score += macd_score

    # 3. 均线评分（-1.5 ~ +1.5）
    ma5 = tech.get("ma5", price)
    ma10 = tech.get("ma10", price)
    ma20 = tech.get("ma20", price)
    if price > ma5 > ma10 > ma20:
        ma_score = 1.5
        signals.append("多头排列")
    elif price < ma5 < ma10 < ma20:
        ma_score = -1.5
        signals.append("空头排列")
    elif price > ma20:
        ma_score = 0.5
    elif price < ma20:
        ma_score = -0.5
    else:
        ma_score = 0.0
    details["ma"] = ma_score
    score += ma_score

    # 4. 布林带评分（-1 ~ +1）
    bb_pos = tech.get("bb_position", 0.5)
    if bb_pos < 0.1:
        bb_score = 1.0
        signals.append("触及布林下轨")
    elif bb_pos < 0.2:
        bb_score = 0.5
    elif bb_pos > 0.9:
        bb_score = -1.0
        signals.append("触及布林上轨")
    elif bb_pos > 0.8:
        bb_score = -0.5
    else:
        bb_score = 0.0
    details["bb"] = bb_score
    score += bb_score

    # 5. 量比评分（-0.5 ~ +0.5）
    vol_ratio = tech.get("volume_ratio", 1.0)
    if vol_ratio > 2.0 and change_pct > 0:
        vol_score = 0.5
        signals.append(f"放量上涨(量比{vol_ratio:.1f}x)")
    elif vol_ratio > 2.0 and change_pct < 0:
        vol_score = -0.5
        signals.append(f"放量下跌(量比{vol_ratio:.1f}x)")
    elif vol_ratio < 0.5:
        vol_score = -0.2
        signals.append("缩量")
    else:
        vol_score = 0.0
    details["volume"] = vol_score
    score += vol_score

    # 6. 金叉/死叉加成
    if tech.get("golden_cross"):
        score += 0.5
        signals.append("均线金叉")
    if tech.get("dead_cross"):
        score -= 0.5
        signals.append("均线死叉")

    # 限制在0-10范围
    score = max(0.0, min(10.0, score))

    return {
        "score": round(score, 1),
        "details": details,
        "signals": signals,
    }


# ============================================================
#  基本面评分
# ============================================================

def score_fundamental(
    symbol: str,
    inventory_trend: dict,
    basis_data: dict,
    positions_data: dict,
    policy_data: dict,
    risk_data: dict,
) -> dict:
    """
    基本面评分（0-10分）
    综合库存趋势、期现价差、机构持仓、政策、风险
    """
    score = 5.0
    details = {}
    signals = []

    # 1. 库存趋势评分（-2 ~ +2）
    inv_signals = inventory_trend.get("signals", [])
    product_name = _symbol_to_product_name(symbol)
    inv_score = 0.0
    for s in inv_signals:
        if s.get("product", "") == product_name:
            trend = s.get("trend", "")
            if "去库" in trend or "下降" in trend:
                inv_score = 2.0
                signals.append(f"库存去化({s.get('change_pct', 0):+.1f}%)")
            elif "增库" in trend or "上升" in trend:
                inv_score = -2.0
                signals.append(f"库存累积({s.get('change_pct', 0):+.1f}%)")
            break
    details["inventory"] = inv_score
    score += inv_score

    # 2. 期现价差评分（-1.5 ~ +1.5）
    basis_score = 0.0
    if basis_data.get("status") == "ok":
        for b in basis_data.get("basis_list", []):
            if b.get("product", "") == product_name:
                basis_pct = b.get("basis_pct", 0)
                # 正基差（现货溢价）说明现货需求强，看涨
                if basis_pct > 2:
                    basis_score = 1.5
                    signals.append(f"正基差强({basis_pct:+.1f}%)")
                elif basis_pct > 0.5:
                    basis_score = 0.5
                    signals.append(f"正基差({basis_pct:+.1f}%)")
                elif basis_pct < -2:
                    basis_score = -1.5
                    signals.append(f"负基差强({basis_pct:+.1f}%)")
                elif basis_pct < -0.5:
                    basis_score = -0.5
                    signals.append(f"负基差({basis_pct:+.1f}%)")
                break
    details["basis"] = basis_score
    score += basis_score

    # 3. 机构持仓评分（-2 ~ +2）
    pos_score = 0.0
    if positions_data.get("status") == "ok":
        pos = positions_data.get("positions", {}).get(symbol, {})
        if pos.get("status") == "ok":
            direction = pos.get("main_direction", "")
            net_change = pos.get("net_long_change", 0)
            if "净多增仓" in direction:
                pos_score = min(2.0, net_change / 10000)
                signals.append(f"主力净多增仓({net_change:+.0f}手)")
            elif "净空增仓" in direction:
                pos_score = max(-2.0, net_change / 10000)
                signals.append(f"主力净空增仓({abs(net_change):.0f}手)")
    details["positions"] = round(pos_score, 2)
    score += pos_score

    # 4. 政策评分（-0.5 ~ +0.5）
    policy_score = 0.0
    if policy_data.get("货币供应"):
        m2 = policy_data["货币供应"].get("M2同比", "")
        try:
            m2_val = float(str(m2).replace("%", ""))
            if m2_val > 10:
                policy_score = 0.5
                signals.append(f"M2宽松({m2})")
            elif m2_val < 7:
                policy_score = -0.5
                signals.append(f"M2偏紧({m2})")
        except Exception:
            pass
    details["policy"] = policy_score
    score += policy_score

    # 5. 风险评分（-1 ~ 0）
    risk_score = 0.0
    if risk_data.get("risk_level") in ["high", "critical"]:
        risk_score = -1.0
        signals.append(f"地缘风险({risk_data.get('risk_level', '')})")
    elif risk_data.get("risk_level") == "medium":
        risk_score = -0.5
    details["risk"] = risk_score
    score += risk_score

    score = max(0.0, min(10.0, score))

    return {
        "score": round(score, 1),
        "details": details,
        "signals": signals,
    }


# ============================================================
#  情绪面评分
# ============================================================

def score_sentiment(
    symbol: str,
    sentiment_data: dict,
    news: list,
    risk_data: dict,
    change_pct: float,
) -> dict:
    """
    情绪面评分（0-10分）
    综合VIX、市场情绪、新闻情绪
    """
    score = 5.0
    details = {}
    signals = []

    # 1. VIX评分（-1 ~ +1）
    vix_score = 0.0
    if sentiment_data.get("VIX"):
        vix = sentiment_data["VIX"].get("value", 20)
        if vix > 30:
            vix_score = -1.0
            signals.append(f"VIX高恐慌({vix:.1f})")
        elif vix > 25:
            vix_score = -0.5
            signals.append(f"VIX偏高({vix:.1f})")
        elif vix < 15:
            vix_score = 0.5
            signals.append(f"VIX低波动({vix:.1f})")
    details["vix"] = vix_score
    score += vix_score

    # 2. 南华商品指数评分（-1 ~ +1）
    nh_score = 0.0
    if sentiment_data.get("南华商品指数"):
        nh_chg = sentiment_data["南华商品指数"].get("change_pct", 0)
        if nh_chg > 1:
            nh_score = 1.0
            signals.append(f"南华指数强势(+{nh_chg:.1f}%)")
        elif nh_chg > 0.3:
            nh_score = 0.5
        elif nh_chg < -1:
            nh_score = -1.0
            signals.append(f"南华指数弱势({nh_chg:.1f}%)")
        elif nh_chg < -0.3:
            nh_score = -0.5
    details["nanhua"] = nh_score
    score += nh_score

    # 3. 美元指数评分（-1 ~ +1）
    dxy_score = 0.0
    if sentiment_data.get("美元指数"):
        dxy = sentiment_data["美元指数"].get("value", 100)
        # 美元强 -> 大宗商品弱（负相关）
        if dxy > 105:
            dxy_score = -1.0
            signals.append(f"美元强势({dxy:.1f})")
        elif dxy > 102:
            dxy_score = -0.5
        elif dxy < 98:
            dxy_score = 1.0
            signals.append(f"美元弱势({dxy:.1f})")
        elif dxy < 100:
            dxy_score = 0.5
    details["dxy"] = dxy_score
    score += dxy_score

    # 4. 新闻情绪评分（-1 ~ +1）
    news_score = _analyze_news_sentiment(news, symbol)
    details["news"] = news_score
    if news_score > 0.3:
        signals.append("新闻偏正面")
    elif news_score < -0.3:
        signals.append("新闻偏负面")
    score += news_score

    # 5. 价格动量评分（-1 ~ +1）
    if change_pct > 2:
        momentum_score = 1.0
        signals.append(f"强势上涨({change_pct:+.1f}%)")
    elif change_pct > 0.5:
        momentum_score = 0.5
    elif change_pct < -2:
        momentum_score = -1.0
        signals.append(f"强势下跌({change_pct:+.1f}%)")
    elif change_pct < -0.5:
        momentum_score = -0.5
    else:
        momentum_score = 0.0
    details["momentum"] = momentum_score
    score += momentum_score

    score = max(0.0, min(10.0, score))

    return {
        "score": round(score, 1),
        "details": details,
        "signals": signals,
    }


def _analyze_news_sentiment(news: list, symbol: str) -> float:
    """简单新闻情绪分析"""
    if not news:
        return 0.0

    positive_words = ["上涨", "增长", "利好", "需求旺盛", "供应紧张", "减产", "去库",
                      "政策支持", "刺激", "复苏", "强劲", "超预期"]
    negative_words = ["下跌", "下降", "利空", "需求疲软", "供应过剩", "增产", "累库",
                      "政策收紧", "衰退", "低迷", "不及预期", "风险"]

    pos_count = 0
    neg_count = 0

    for n in news[:10]:
        title = n.get("title", "")
        for w in positive_words:
            if w in title:
                pos_count += 1
        for w in negative_words:
            if w in title:
                neg_count += 1

    total = pos_count + neg_count
    if total == 0:
        return 0.0

    sentiment = (pos_count - neg_count) / total
    return round(sentiment, 2)


# ============================================================
#  综合评分
# ============================================================

def calculate_composite_score(
    tech_score: dict,
    fundamental_score: dict,
    sentiment_score: dict,
) -> dict:
    """
    计算综合评分（1-10分）
    """
    t_score = tech_score.get("score", 5.0)
    f_score = fundamental_score.get("score", 5.0)
    s_score = sentiment_score.get("score", 5.0)

    composite = (
        t_score * SCORE_WEIGHTS["technical"] +
        f_score * SCORE_WEIGHTS["fundamental"] +
        s_score * SCORE_WEIGHTS["sentiment"]
    )
    composite = round(max(1.0, min(10.0, composite)), 1)

    # 判断信号等级
    level = "中性"
    recommendation = "中性"
    for (low, high), label in SIGNAL_LEVELS.items():
        if low <= composite <= high:
            level = label
            recommendation = label
            break

    # 多维度共振检测
    resonance = _detect_resonance(tech_score, fundamental_score, sentiment_score)

    return {
        "composite_score": composite,
        "technical_score": t_score,
        "fundamental_score": f_score,
        "sentiment_score": s_score,
        "level": level,
        "recommendation": recommendation,
        "resonance": resonance,
        "all_signals": (
            tech_score.get("signals", []) +
            fundamental_score.get("signals", []) +
            sentiment_score.get("signals", [])
        ),
    }


def _detect_resonance(tech: dict, fundamental: dict, sentiment: dict) -> str:
    """检测多维度共振"""
    t = tech.get("score", 5)
    f = fundamental.get("score", 5)
    s = sentiment.get("score", 5)

    all_bullish = t > 6.5 and f > 6.5 and s > 6.5
    all_bearish = t < 3.5 and f < 3.5 and s < 3.5
    two_bullish = sum([t > 6.5, f > 6.5, s > 6.5]) >= 2
    two_bearish = sum([t < 3.5, f < 3.5, s < 3.5]) >= 2

    if all_bullish:
        return "三维共振看涨⭐⭐⭐"
    elif all_bearish:
        return "三维共振看跌⭐⭐⭐"
    elif two_bullish:
        return "双维共振看涨⭐⭐"
    elif two_bearish:
        return "双维共振看跌⭐⭐"
    else:
        return "无明显共振"


# ============================================================
#  历史胜率统计
# ============================================================

def load_signal_history() -> dict:
    """加载历史信号记录"""
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"signals": [], "win_rates": {}}


def save_signal_history(history: dict):
    """保存历史信号记录"""
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def record_signal(symbol: str, score: float, recommendation: str, price: float):
    """记录信号（用于后续胜率统计）"""
    history = load_signal_history()
    signal_record = {
        "symbol": symbol,
        "score": score,
        "recommendation": recommendation,
        "price": price,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "result": None,  # 待后续验证
    }
    history["signals"].append(signal_record)
    # 只保留最近500条
    history["signals"] = history["signals"][-500:]
    save_signal_history(history)


def get_win_rate(symbol: str, recommendation: str) -> Optional[float]:
    """
    获取历史胜率
    返回: 0.0-1.0 的胜率，或 None（数据不足）
    """
    history = load_signal_history()
    signals = history.get("signals", [])

    # 筛选相同品种和推荐方向的历史信号
    relevant = [
        s for s in signals
        if s.get("symbol") == symbol
        and s.get("recommendation") == recommendation
        and s.get("result") is not None
    ]

    if len(relevant) < 5:
        return None  # 数据不足

    wins = sum(1 for s in relevant if s.get("result") == "win")
    return round(wins / len(relevant), 2)


def update_signal_results(categories_data: dict):
    """
    更新历史信号结果（对比当前价格与记录价格）
    """
    history = load_signal_history()
    signals = history.get("signals", [])
    updated = False

    cutoff = datetime.now() - timedelta(days=3)

    for signal in signals:
        if signal.get("result") is not None:
            continue

        # 检查是否超过3天（可以验证结果了）
        try:
            sig_time = datetime.strptime(signal["timestamp"], "%Y-%m-%d %H:%M:%S")
            if sig_time > cutoff:
                continue
        except Exception:
            continue

        symbol = signal.get("symbol", "")
        rec = signal.get("recommendation", "")
        old_price = signal.get("price", 0)

        # 获取当前价格
        current_info = categories_data.get(symbol, {})
        current_price = current_info.get("price", 0)

        if current_price > 0 and old_price > 0:
            price_change = (current_price - old_price) / old_price

            # 判断胜负
            if "推荐" in rec or "偏多" in rec:
                signal["result"] = "win" if price_change > 0.005 else "loss"
            elif "谨慎" in rec or "偏空" in rec:
                signal["result"] = "win" if price_change < -0.005 else "loss"
            else:
                signal["result"] = "neutral"

            signal["result_price"] = current_price
            signal["result_change_pct"] = round(price_change * 100, 2)
            updated = True

    if updated:
        save_signal_history(history)


# ============================================================
#  主评分函数
# ============================================================

def score_all_symbols(
    categories_data: dict,
    inventory_trend: dict,
    basis_data: dict,
    positions_data: dict,
    policy_data: dict,
    sentiment_data: dict,
    risk_data: dict,
    news: list,
) -> dict:
    """
    对所有品种进行综合评分
    返回: {symbol: composite_score_dict}
    """
    print(f"\n🎯 开始信号强度评分...")

    # 更新历史信号结果
    update_signal_results(categories_data)

    scores = {}
    for symbol, info in categories_data.items():
        if info.get("status") != "OK":
            continue

        tech = info.get("tech", {})
        price = info.get("price", 0)
        change_pct = info.get("change_pct", 0)

        # 三维评分
        t_score = score_technical(tech, price, change_pct)
        f_score = score_fundamental(
            symbol, inventory_trend, basis_data,
            positions_data, policy_data, risk_data
        )
        s_score = score_sentiment(symbol, sentiment_data, news, risk_data, change_pct)

        # 综合评分
        composite = calculate_composite_score(t_score, f_score, s_score)

        # 获取历史胜率
        win_rate = get_win_rate(symbol, composite["recommendation"])
        composite["win_rate"] = win_rate

        # 记录本次信号
        record_signal(symbol, composite["composite_score"], composite["recommendation"], price)

        scores[symbol] = composite

    # 按综合评分排序
    sorted_scores = dict(sorted(scores.items(),
                                key=lambda x: x[1].get("composite_score", 5),
                                reverse=True))

    # 统计
    strong_buy = [s for s, v in sorted_scores.items() if v.get("composite_score", 0) >= 8]
    strong_sell = [s for s, v in sorted_scores.items() if v.get("composite_score", 0) <= 2]
    resonance_signals = [s for s, v in sorted_scores.items()
                         if "三维共振" in v.get("resonance", "")]

    print(f"  ✅ 评分完成: {len(scores)} 个品种")
    print(f"  → 强烈推荐: {len(strong_buy)} | 谨慎: {len(strong_sell)} | 三维共振: {len(resonance_signals)}")

    return {
        "status": "ok",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scores": sorted_scores,
        "summary": {
            "total": len(scores),
            "strong_buy": strong_buy[:5],
            "strong_sell": strong_sell[:5],
            "resonance_signals": resonance_signals[:3],
        }
    }


def _symbol_to_product_name(symbol: str) -> str:
    """期货代码转品种名"""
    # 直接使用 basis_analysis 中的 FUTURES_TO_PRODUCT（避免循环导入）
    from basis_analysis import FUTURES_TO_PRODUCT
    product_code = symbol[:2].upper() if len(symbol) >= 2 and symbol[:2].isalpha() else symbol[:1].upper()
    return FUTURES_TO_PRODUCT.get(product_code, "")


def format_scores_text(scores_data: dict) -> str:
    """格式化评分文本（供LLM使用）"""
    if scores_data.get("status") != "ok":
        return "信号评分数据暂不可用"

    lines = ["【信号强度评分】"]
    summary = scores_data.get("summary", {})

    strong_buy = summary.get("strong_buy", [])
    if strong_buy:
        lines.append(f"\n强烈推荐品种 (评分≥8):")
        for s in strong_buy:
            score_info = scores_data["scores"].get(s, {})
            score = score_info.get("composite_score", 0)
            resonance = score_info.get("resonance", "")
            win_rate = score_info.get("win_rate")
            wr_str = f" 历史胜率:{win_rate:.0%}" if win_rate else ""
            lines.append(f"  {s}: {score}分 [{resonance}]{wr_str}")

    strong_sell = summary.get("strong_sell", [])
    if strong_sell:
        lines.append(f"\n谨慎品种 (评分≤2):")
        for s in strong_sell:
            score_info = scores_data["scores"].get(s, {})
            score = score_info.get("composite_score", 0)
            lines.append(f"  {s}: {score}分")

    resonance = summary.get("resonance_signals", [])
    if resonance:
        lines.append(f"\n三维共振信号: {', '.join(resonance)}")

    # 前10名详细
    lines.append("\n评分排行 (前10):")
    for i, (symbol, score_info) in enumerate(list(scores_data["scores"].items())[:10]):
        score = score_info.get("composite_score", 0)
        rec = score_info.get("recommendation", "")
        t = score_info.get("technical_score", 0)
        f = score_info.get("fundamental_score", 0)
        s = score_info.get("sentiment_score", 0)
        lines.append(f"  {i+1}. {symbol}: {score}分 [{rec}] "
                     f"(技{t}/基{f}/情{s})")

    return "\n".join(lines)
