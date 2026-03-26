# ==================== 机构持仓排名模块 v1.0 ====================
"""
institutional_positions.py
从交易所数据爬取主力持仓排名，识别主力加仓/减仓信号
支持多接口降级，错误处理完善
"""

import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

# ============================================================
#  交易所持仓数据配置
# ============================================================

# 主要期货品种 -> 交易所映射
PRODUCT_EXCHANGE_MAP = {
    # 上期所 (SHFE)
    "RB": "上期所", "HC": "上期所", "CU": "上期所", "AL": "上期所",
    "ZN": "上期所", "NI": "上期所", "SN": "上期所", "AU": "上期所",
    "AG": "上期所", "FU": "上期所", "BU": "上期所", "RU": "上期所",
    # 大商所 (DCE)
    "I": "大商所", "J": "大商所", "JM": "大商所", "M": "大商所",
    "Y": "大商所", "P": "大商所", "C": "大商所", "CS": "大商所",
    "L": "大商所", "V": "大商所", "PP": "大商所", "EB": "大商所",
    # 郑商所 (CZCE)
    "SR": "郑商所", "CF": "郑商所", "TA": "郑商所", "MA": "郑商所",
    "AP": "郑商所", "CJ": "郑商所", "RM": "郑商所", "OI": "郑商所",
    "ZC": "郑商所", "FG": "郑商所", "SA": "郑商所",
    # 中金所 (CFFEX)
    "IF": "中金所", "IC": "中金所", "IH": "中金所", "IM": "中金所",
    "T": "中金所", "TF": "中金所", "TS": "中金所",
    # 上期能源 (INE)
    "SC": "上期能源", "LU": "上期能源", "NR": "上期能源",
}

# 主力席位阈值（持仓量超过此值认为是主力）
MAJOR_POSITION_THRESHOLD = 5000

# 加仓/减仓信号阈值（变化量超过此比例）
POSITION_CHANGE_THRESHOLD = 0.05  # 5%


# ============================================================
#  持仓数据采集
# ============================================================

def _fetch_positions_akshare(symbol: str, date_str: str = None) -> Optional[pd.DataFrame]:
    """通过akshare获取持仓排名数据"""
    try:
        import akshare as ak
        time.sleep(0.5)

        if date_str is None:
            date_str = datetime.now().strftime("%Y%m%d")

        # 策略1：期货持仓排名（按品种）
        product = symbol[:2] if symbol[:2].isalpha() else symbol[:1]

        df = ak.futures_positions_rank_czce(date=date_str, symbol=product)
        if df is not None and len(df) > 0:
            return df

    except Exception:
        pass

    try:
        import akshare as ak
        time.sleep(0.5)
        # 策略2：上期所持仓
        df2 = ak.futures_positions_rank_shfe(date=date_str, symbol=symbol)
        if df2 is not None and len(df2) > 0:
            return df2
    except Exception:
        pass

    try:
        import akshare as ak
        time.sleep(0.5)
        # 策略3：大商所持仓
        df3 = ak.futures_positions_rank_dce(date=date_str, symbol=symbol)
        if df3 is not None and len(df3) > 0:
            return df3
    except Exception:
        pass

    return None


def _fetch_positions_em(symbol: str) -> Optional[pd.DataFrame]:
    """通过东财获取持仓数据"""
    try:
        import akshare as ak
        time.sleep(0.5)
        df = ak.futures_hold_pos_sina(symbol=symbol)
        if df is not None and len(df) > 0:
            return df
    except Exception:
        pass
    return None


def get_institutional_positions(symbols: list) -> dict:
    """
    获取多个品种的机构持仓排名
    返回: {symbol: {"top_long": [...], "top_short": [...], "net_change": float}}
    """
    positions = {}
    print(f"\n🏦 采集机构持仓排名...")

    today = datetime.now().strftime("%Y%m%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    for symbol in symbols[:10]:  # 限制数量
        product = symbol[:2].upper() if symbol[:2].isalpha() else symbol[:1].upper()

        df = _fetch_positions_akshare(symbol, today)
        if df is None:
            df = _fetch_positions_akshare(symbol, yesterday)
        if df is None:
            df = _fetch_positions_em(symbol)

        if df is not None and len(df) > 0:
            try:
                pos_data = _parse_position_df(df, symbol)
                positions[symbol] = pos_data
                net = pos_data.get("net_long_change", 0)
                print(f"  ✅ {symbol}: 净多变化 {net:+.0f} 手")
            except Exception as e:
                positions[symbol] = {"status": "解析失败", "error": str(e)}
                print(f"  ⚠️ {symbol}: 解析失败")
        else:
            positions[symbol] = {"status": "数据不可用"}
            print(f"  ⚠️ {symbol}: 持仓数据不可用")

    return positions


def _parse_position_df(df: pd.DataFrame, symbol: str) -> dict:
    """解析持仓DataFrame"""
    # 标准化列名
    col_map = {
        "会员简称": "member", "成员简称": "member", "席位": "member",
        "多单持仓量": "long_pos", "多头持仓": "long_pos", "买持仓量": "long_pos",
        "空单持仓量": "short_pos", "空头持仓": "short_pos", "卖持仓量": "short_pos",
        "多单增减": "long_change", "多头增减": "long_change",
        "空单增减": "short_change", "空头增减": "short_change",
        "持仓量": "total_pos", "净持仓": "net_pos",
    }

    df_renamed = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    top_long = []
    top_short = []
    total_long_change = 0
    total_short_change = 0

    for _, row in df_renamed.head(20).iterrows():
        member = str(row.get("member", row.get("会员简称", "未知")))[:20]

        long_pos = _safe_int(row.get("long_pos", 0))
        short_pos = _safe_int(row.get("short_pos", 0))
        long_change = _safe_int(row.get("long_change", 0))
        short_change = _safe_int(row.get("short_change", 0))

        if long_pos > 0:
            top_long.append({
                "member": member,
                "position": long_pos,
                "change": long_change,
                "signal": _position_signal(long_change, long_pos),
            })
            total_long_change += long_change

        if short_pos > 0:
            top_short.append({
                "member": member,
                "position": short_pos,
                "change": short_change,
                "signal": _position_signal(short_change, short_pos),
            })
            total_short_change += short_change

    # 按持仓量排序
    top_long.sort(key=lambda x: x["position"], reverse=True)
    top_short.sort(key=lambda x: x["position"], reverse=True)

    net_long_change = total_long_change - total_short_change

    # 判断主力方向
    if net_long_change > MAJOR_POSITION_THRESHOLD:
        main_direction = "主力净多增仓"
        price_signal = "看涨信号"
    elif net_long_change < -MAJOR_POSITION_THRESHOLD:
        main_direction = "主力净空增仓"
        price_signal = "看跌信号"
    elif total_long_change > 0 and total_short_change > 0:
        main_direction = "多空双增"
        price_signal = "分歧加大"
    elif total_long_change < 0 and total_short_change < 0:
        main_direction = "多空双减"
        price_signal = "观望情绪"
    else:
        main_direction = "持仓稳定"
        price_signal = "中性"

    return {
        "symbol": symbol,
        "status": "ok",
        "top_long": top_long[:10],
        "top_short": top_short[:10],
        "total_long_change": total_long_change,
        "total_short_change": total_short_change,
        "net_long_change": net_long_change,
        "main_direction": main_direction,
        "price_signal": price_signal,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }


def _safe_int(val) -> int:
    """安全转换为整数"""
    try:
        if pd.isna(val):
            return 0
        return int(float(str(val).replace(",", "").replace("--", "0")))
    except Exception:
        return 0


def _position_signal(change: int, position: int) -> str:
    """判断持仓变化信号"""
    if position == 0:
        return "无"
    ratio = change / position if position > 0 else 0
    if change > MAJOR_POSITION_THRESHOLD:
        return "大幅加仓"
    elif change > 0 and ratio > POSITION_CHANGE_THRESHOLD:
        return "加仓"
    elif change < -MAJOR_POSITION_THRESHOLD:
        return "大幅减仓"
    elif change < 0 and abs(ratio) > POSITION_CHANGE_THRESHOLD:
        return "减仓"
    else:
        return "持平"


# ============================================================
#  主力资金流向分析
# ============================================================

def analyze_money_flow(positions_data: dict) -> dict:
    """
    分析主力资金流向
    返回: 整体资金流向、重点品种信号
    """
    bullish_symbols = []
    bearish_symbols = []
    divergence_symbols = []

    for symbol, pos in positions_data.items():
        if pos.get("status") != "ok":
            continue

        direction = pos.get("main_direction", "")
        net_change = pos.get("net_long_change", 0)

        if "净多增仓" in direction:
            bullish_symbols.append({
                "symbol": symbol,
                "net_change": net_change,
                "signal": pos.get("price_signal", ""),
            })
        elif "净空增仓" in direction:
            bearish_symbols.append({
                "symbol": symbol,
                "net_change": net_change,
                "signal": pos.get("price_signal", ""),
            })
        elif "双增" in direction:
            divergence_symbols.append({
                "symbol": symbol,
                "signal": "多空分歧",
            })

    # 按净变化量排序
    bullish_symbols.sort(key=lambda x: x["net_change"], reverse=True)
    bearish_symbols.sort(key=lambda x: abs(x["net_change"]), reverse=True)

    overall_sentiment = "中性"
    if len(bullish_symbols) > len(bearish_symbols) * 1.5:
        overall_sentiment = "偏多"
    elif len(bearish_symbols) > len(bullish_symbols) * 1.5:
        overall_sentiment = "偏空"

    return {
        "overall_sentiment": overall_sentiment,
        "bullish_symbols": bullish_symbols[:5],
        "bearish_symbols": bearish_symbols[:5],
        "divergence_symbols": divergence_symbols[:3],
        "total_analyzed": len([p for p in positions_data.values() if p.get("status") == "ok"]),
    }


def get_positions_summary(categories_data: dict) -> dict:
    """
    主入口：获取所有品种的机构持仓分析
    输入: categories_data（来自data_collector）
    输出: 结构化持仓分析结果
    """
    print(f"\n🏦 开始机构持仓分析...")

    # 提取有效品种代码
    valid_symbols = [
        symbol for symbol, info in categories_data.items()
        if info.get("status") == "OK"
    ]

    if not valid_symbols:
        return {"status": "no_data", "positions": {}, "money_flow": {}}

    # 获取持仓数据
    positions_data = get_institutional_positions(valid_symbols[:15])

    # 分析资金流向
    money_flow = analyze_money_flow(positions_data)

    print(f"  ✅ 持仓分析完成: {money_flow.get('total_analyzed', 0)} 个品种")
    print(f"  → 整体情绪: {money_flow.get('overall_sentiment', '-')}")

    return {
        "status": "ok",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "positions": positions_data,
        "money_flow": money_flow,
    }


def format_positions_text(positions_summary: dict) -> str:
    """格式化持仓分析文本（供LLM使用）"""
    if positions_summary.get("status") != "ok":
        return "机构持仓数据暂不可用"

    lines = ["【机构持仓排名】"]
    money_flow = positions_summary.get("money_flow", {})

    lines.append(f"整体情绪: {money_flow.get('overall_sentiment', '-')}")
    lines.append(f"分析品种: {money_flow.get('total_analyzed', 0)}")

    bullish = money_flow.get("bullish_symbols", [])
    if bullish:
        lines.append("\n主力净多增仓（看涨）:")
        for b in bullish[:5]:
            lines.append(f"  {b['symbol']}: 净多变化 {b['net_change']:+.0f} 手")

    bearish = money_flow.get("bearish_symbols", [])
    if bearish:
        lines.append("\n主力净空增仓（看跌）:")
        for b in bearish[:5]:
            lines.append(f"  {b['symbol']}: 净空变化 {abs(b['net_change']):.0f} 手")

    # 详细持仓
    positions = positions_summary.get("positions", {})
    for symbol, pos in list(positions.items())[:5]:
        if pos.get("status") != "ok":
            continue
        lines.append(f"\n{symbol} [{pos.get('main_direction', '-')}]:")
        top_long = pos.get("top_long", [])[:3]
        for m in top_long:
            lines.append(f"  多: {m['member']} {m['position']}手 ({m['signal']})")
        top_short = pos.get("top_short", [])[:3]
        for m in top_short:
            lines.append(f"  空: {m['member']} {m['position']}手 ({m['signal']})")

    return "\n".join(lines)
