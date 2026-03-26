# ==================== 期现价差分析模块 v2.0 ====================
"""
basis_analysis.py
期货 vs 现货价差分析：基差趋势、套利机会识别
使用 akshare futures_spot_price 获取真实现货价格
v2.0: 删除硬编码 fallback 价格，现货不可用时明确标注
"""

import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

# ============================================================
#  期货合约 -> 品种名映射（与 institutional_positions 保持一致）
# ============================================================

FUTURES_TO_PRODUCT = {
    "RB": "螺纹钢", "HC": "热轧卷板", "I": "铁矿石", "J": "焦炭", "JM": "焦煤",
    "CU": "铜", "AL": "铝", "ZN": "锌", "NI": "镍", "SN": "锡",
    "SC": "原油", "MA": "甲醇", "TA": "PTA", "FU": "燃料油",
    "M": "豆粕", "Y": "豆油", "P": "棕榈油", "C": "玉米", "SR": "白糖",
    "CF": "棉花", "AP": "苹果",
}

# futures_spot_price 接口中的 symbol 字段（期货代码大写）
# 该接口返回的 symbol 列就是期货代码（如 RB, CU, M 等）
SPOT_PRICE_SYMBOL_MAP = {
    "RB": "RB", "HC": "HC", "I": "I", "J": "J", "JM": "JM",
    "CU": "CU", "AL": "AL", "ZN": "ZN", "NI": "NI", "SN": "SN",
    "SC": "SC", "MA": "MA", "TA": "TA", "FU": "FU",
    "M": "M", "Y": "Y", "P": "P", "C": "C", "SR": "SR",
    "CF": "CF", "AP": "AP",
}

# 品种单位
PRODUCT_UNIT = {
    "螺纹钢": "元/吨", "热轧卷板": "元/吨", "铁矿石": "元/吨", "焦炭": "元/吨", "焦煤": "元/吨",
    "铜": "元/吨", "铝": "元/吨", "锌": "元/吨", "镍": "元/吨", "锡": "元/吨",
    "原油": "美元/桶", "甲醇": "元/吨", "PTA": "元/吨", "燃料油": "元/吨",
    "豆粕": "元/吨", "豆油": "元/吨", "棕榈油": "元/吨", "玉米": "元/吨",
    "白糖": "元/吨", "棉花": "元/吨", "苹果": "元/吨",
}


# ============================================================
#  现货价格采集（使用 futures_spot_price 真实接口）
# ============================================================

def _get_recent_trading_date() -> str:
    """获取最近的交易日（今天或往前推）"""
    today = datetime.now()
    # 往前最多找7天
    for i in range(7):
        d = today - timedelta(days=i)
        # 跳过周末
        if d.weekday() < 5:  # 0=周一, 4=周五
            return d.strftime('%Y%m%d')
    return today.strftime('%Y%m%d')


def get_spot_prices_from_akshare(product_codes: list) -> dict:
    """
    通过 akshare futures_spot_price 获取真实现货价格
    product_codes: 期货代码列表，如 ['RB', 'CU', 'M']
    返回: {product_code: {"spot_price": float, "date": str, "product_name": str}}
    """
    import akshare as ak

    spot_data = {}
    print(f"\n💰 采集现货价格（futures_spot_price）...")

    # 尝试最近几个交易日
    for days_back in range(5):
        target_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
        # 跳过周末
        d = datetime.now() - timedelta(days=days_back)
        if d.weekday() >= 5:
            continue

        try:
            time.sleep(0.5)
            df = ak.futures_spot_price(date=target_date, vars_list=product_codes)
            if df is not None and len(df) > 0:
                print(f"  ✅ 获取到 {target_date} 的现货数据，共 {len(df)} 条")
                for _, row in df.iterrows():
                    sym = str(row.get('symbol', '')).upper()
                    spot_price = row.get('spot_price', None)
                    if sym and spot_price is not None:
                        try:
                            price_val = float(spot_price)
                            if price_val > 0:
                                product_name = FUTURES_TO_PRODUCT.get(sym, sym)
                                spot_data[sym] = {
                                    "spot_price": price_val,
                                    "date": target_date,
                                    "product_name": product_name,
                                    "unit": PRODUCT_UNIT.get(product_name, "元/吨"),
                                    "source": "futures_spot_price",
                                }
                        except (ValueError, TypeError):
                            pass
                if spot_data:
                    break
        except Exception as e:
            print(f"  ⚠️ {target_date} 获取失败: {e}")
            continue

    if not spot_data:
        print(f"  ❌ 现货价格获取失败（所有日期均无数据）")

    return spot_data


def get_spot_prices(products: list) -> dict:
    """
    获取多个品种的现货价格
    products: 品种名列表（中文），如 ['螺纹钢', '铜']
    返回: {product_name: {"price": float, "source": str, "unit": str, "available": bool}}
    """
    # 将品种名转换为期货代码
    name_to_code = {v: k for k, v in FUTURES_TO_PRODUCT.items()}
    product_codes = []
    code_to_name = {}
    for p in products:
        code = name_to_code.get(p)
        if code:
            product_codes.append(code)
            code_to_name[code] = p

    if not product_codes:
        return {}

    # 获取现货数据
    raw_data = get_spot_prices_from_akshare(product_codes)

    # 转换为品种名索引
    result = {}
    for code, name in code_to_name.items():
        if code in raw_data:
            d = raw_data[code]
            result[name] = {
                "price": d["spot_price"],
                "source": d["source"],
                "unit": d["unit"],
                "date": d["date"],
                "available": True,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
            }
            print(f"  ✅ {name}: {d['spot_price']} {d['unit']} [来源: {d['source']}, 日期: {d['date']}]")
        else:
            # 现货数据不可用，明确标注，不使用假数据
            result[name] = {
                "price": None,
                "source": "unavailable",
                "unit": PRODUCT_UNIT.get(name, "元/吨"),
                "date": None,
                "available": False,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
            }
            print(f"  ❌ {name}: 现货数据不可用")

    return result


# ============================================================
#  基差计算
# ============================================================

def calculate_basis(futures_price: float, spot_price: float) -> dict:
    """
    计算基差
    基差 = 现货价格 - 期货价格
    正基差（现货溢价）：现货 > 期货，期货升水
    负基差（期货溢价）：期货 > 现货，期货贴水
    """
    basis = spot_price - futures_price
    basis_pct = (basis / spot_price * 100) if spot_price != 0 else 0

    if basis > 0:
        basis_type = "正基差"  # 现货溢价，期货升水
        signal = "期货相对低估，正向套利机会"
    elif basis < 0:
        basis_type = "负基差"  # 期货溢价，期货贴水
        signal = "期货相对高估，反向套利机会"
    else:
        basis_type = "平价"
        signal = "期现价格一致"

    # 基差绝对值判断
    abs_pct = abs(basis_pct)
    if abs_pct > 3:
        arb_level = "强套利机会"
    elif abs_pct > 1.5:
        arb_level = "中等套利机会"
    elif abs_pct > 0.5:
        arb_level = "弱套利机会"
    else:
        arb_level = "无明显套利"

    return {
        "basis": round(basis, 2),
        "basis_pct": round(basis_pct, 2),
        "basis_type": basis_type,
        "signal": signal,
        "arb_level": arb_level,
    }


def get_historical_basis(product: str, days: int = 30) -> list:
    """
    获取历史基差数据（用于趋势分析）
    返回: [{"date": str, "basis": float, "basis_pct": float}, ...]
    """
    history = []
    try:
        import akshare as ak
        time.sleep(0.3)

        # 获取历史现货价格（使用 futures_spot_price_daily）
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

        # 将品种名转换为期货代码
        name_to_code = {v: k for k, v in FUTURES_TO_PRODUCT.items()}
        code = name_to_code.get(product)
        if not code:
            return []

        df_spot = ak.futures_spot_price_daily(
            start_day=start_date, end_day=end_date, vars_list=[code]
        )

        if df_spot is not None and len(df_spot) > 3:
            for _, row in df_spot.iterrows():
                spot_p = float(row.get("spot_price", 0))
                date_str = str(row.get("date", ""))[:10]
                if spot_p > 0 and date_str:
                    history.append({
                        "date": date_str,
                        "spot_price": spot_p,
                        "basis": None,
                        "basis_pct": None
                    })

    except Exception:
        pass

    return history


# ============================================================
#  基差趋势分析
# ============================================================

def analyze_basis_trend(basis_history: list) -> dict:
    """
    分析基差趋势
    返回: 趋势方向、扩大/收窄信号
    """
    if len(basis_history) < 3:
        return {"trend": "数据不足", "signal": "无法判断"}

    recent = [b["basis_pct"] for b in basis_history[-5:] if b.get("basis_pct") is not None]
    if len(recent) < 2:
        return {"trend": "数据不足", "signal": "无法判断"}

    # 计算趋势
    avg_recent = sum(recent[-3:]) / len(recent[-3:])
    avg_earlier = sum(recent[:2]) / 2

    if avg_recent > avg_earlier + 0.3:
        trend = "基差扩大"
        signal = "现货相对走强，期货可能补涨"
    elif avg_recent < avg_earlier - 0.3:
        trend = "基差收窄"
        signal = "期货相对走强，现货可能补跌"
    else:
        trend = "基差稳定"
        signal = "期现关系正常"

    return {
        "trend": trend,
        "signal": signal,
        "recent_avg_pct": round(avg_recent, 2),
        "earlier_avg_pct": round(avg_earlier, 2),
    }


# ============================================================
#  主分析函数
# ============================================================

def analyze_basis(categories_data: dict) -> dict:
    """
    主函数：分析所有品种的期现价差
    输入: categories_data（来自data_collector）
    输出: 结构化基差分析结果
    """
    print(f"\n📊 开始期现价差分析...")

    # 提取有效品种
    valid_products = []
    futures_prices = {}

    for symbol, info in categories_data.items():
        if info.get("status") != "OK":
            continue
        product = info.get("product", "")
        # 将期货代码转换为品种名
        product_name = FUTURES_TO_PRODUCT.get(product.upper(), "")
        if product_name:
            valid_products.append(product_name)
            futures_prices[product_name] = {
                "futures_price": info.get("price", 0),
                "symbol": symbol,
                "change_pct": info.get("change_pct", 0),
            }

    # 去重
    valid_products = list(set(valid_products))

    if not valid_products:
        print("  ⚠️ 无可分析的品种")
        return {"status": "no_data", "basis_list": [], "arb_opportunities": []}

    # 获取现货价格（真实数据，不使用 fallback）
    spot_prices = get_spot_prices(valid_products[:15])  # 限制数量避免超时

    # 计算基差
    basis_results = []
    arb_opportunities = []
    unavailable_products = []

    for product_name, spot_info in spot_prices.items():
        if product_name not in futures_prices:
            continue

        fut_info = futures_prices[product_name]
        fut_price = fut_info["futures_price"]

        # 现货数据不可用时，明确标注，不计算基差
        if not spot_info.get("available") or spot_info.get("price") is None:
            unavailable_products.append(product_name)
            continue

        spot_price = spot_info["price"]

        if fut_price <= 0 or spot_price <= 0:
            continue

        basis_info = calculate_basis(fut_price, spot_price)

        result = {
            "product": product_name,
            "symbol": fut_info["symbol"],
            "futures_price": fut_price,
            "spot_price": spot_price,
            "spot_date": spot_info.get("date", ""),
            "unit": spot_info["unit"],
            "spot_source": spot_info["source"],
            **basis_info,
            "futures_change_pct": fut_info["change_pct"],
        }
        basis_results.append(result)

        # 识别套利机会
        if basis_info["arb_level"] in ["强套利机会", "中等套利机会"]:
            arb_opportunities.append({
                "product": product_name,
                "basis": basis_info["basis"],
                "basis_pct": basis_info["basis_pct"],
                "basis_type": basis_info["basis_type"],
                "arb_level": basis_info["arb_level"],
                "strategy": _get_arb_strategy(basis_info),
            })

    # 按基差绝对值排序
    basis_results.sort(key=lambda x: abs(x.get("basis_pct", 0)), reverse=True)
    arb_opportunities.sort(key=lambda x: abs(x.get("basis_pct", 0)), reverse=True)

    # 统计
    positive_basis = [b for b in basis_results if b.get("basis", 0) > 0]
    negative_basis = [b for b in basis_results if b.get("basis", 0) < 0]

    summary = {
        "total_analyzed": len(basis_results),
        "positive_basis_count": len(positive_basis),
        "negative_basis_count": len(negative_basis),
        "arb_count": len(arb_opportunities),
        "unavailable_count": len(unavailable_products),
        "unavailable_products": unavailable_products,
        "market_structure": _judge_market_structure(basis_results),
    }

    if unavailable_products:
        print(f"  ⚠️ 现货数据不可用品种: {', '.join(unavailable_products)}")
    print(f"  ✅ 分析完成: {len(basis_results)} 个品种, {len(arb_opportunities)} 个套利机会")

    return {
        "status": "ok",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "basis_list": basis_results,
        "arb_opportunities": arb_opportunities,
        "summary": summary,
    }


def _get_arb_strategy(basis_info: dict) -> str:
    """根据基差信息生成套利策略建议"""
    basis_type = basis_info.get("basis_type", "")
    arb_level = basis_info.get("arb_level", "")

    if basis_type == "正基差":
        return "买期货+卖现货（正向套利）"
    elif basis_type == "负基差":
        return "卖期货+买现货（反向套利）"
    else:
        return "观望"


def _judge_market_structure(basis_results: list) -> str:
    """判断整体市场结构"""
    if not basis_results:
        return "数据不足"

    positive = sum(1 for b in basis_results if b.get("basis", 0) > 0)
    total = len(basis_results)
    ratio = positive / total if total > 0 else 0.5

    if ratio > 0.7:
        return "整体现货溢价（期货升水为主）"
    elif ratio < 0.3:
        return "整体期货溢价（期货贴水为主）"
    else:
        return "期现结构分化"


def format_basis_text(basis_data: dict) -> str:
    """格式化基差分析文本（供LLM使用）"""
    if basis_data.get("status") != "ok":
        return "期现价差数据暂不可用"

    lines = ["【期现价差分析】"]
    summary = basis_data.get("summary", {})
    lines.append(f"市场结构: {summary.get('market_structure', '-')}")
    lines.append(f"分析品种: {summary.get('total_analyzed', 0)} | "
                 f"正基差: {summary.get('positive_basis_count', 0)} | "
                 f"负基差: {summary.get('negative_basis_count', 0)}")

    unavailable = summary.get("unavailable_products", [])
    if unavailable:
        lines.append(f"现货数据不可用: {', '.join(unavailable)}")

    arb_ops = basis_data.get("arb_opportunities", [])
    if arb_ops:
        lines.append(f"\n套利机会 ({len(arb_ops)} 个):")
        for op in arb_ops[:5]:
            lines.append(f"  {op['product']}: 基差{op['basis_pct']:+.2f}% "
                         f"[{op['basis_type']}] {op['arb_level']} → {op['strategy']}")

    basis_list = basis_data.get("basis_list", [])
    if basis_list:
        lines.append("\n主要品种基差:")
        for b in basis_list[:8]:
            spot_date = b.get('spot_date', '')
            date_str = f" (现货日期:{spot_date})" if spot_date else ""
            lines.append(f"  {b['product']}: 期{b['futures_price']} vs 现{b['spot_price']} "
                         f"基差{b['basis_pct']:+.2f}% [{b['basis_type']}]{date_str}")

    return "\n".join(lines)
