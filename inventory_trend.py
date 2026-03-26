# ==================== 库存环比趋势分析模块 v1.0 ====================
"""
库存环比趋势分析
- 计算库存周环比/月环比变化率
- 判断库存增减趋势（去库/增库/平稳）
- 结合价格变动，给出库存-价格背离信号
- 返回结构化数据供 AI 分析使用
"""

import time
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
from typing import Optional, Dict, List


# ============================================================
# 期货品种库存配置
# ============================================================

INVENTORY_PRODUCTS = {
    "螺纹钢": "螺纹钢",
    "热轧卷板": "热轧卷板",
    "铁矿石": "铁矿石",
    "铜": "铜",
    "铝": "铝",
    "锌": "锌",
    "镍": "镍",
    "甲醇": "甲醇",
    "PTA": "PTA",
    "乙二醇": "乙二醇",
    "豆粕": "豆粕",
    "豆油": "豆油",
    "棕榈油": "棕榈油",
    "玉米": "玉米",
    "白糖": "白糖",
    "棉花": "棉花",
}

INVENTORY_SEASONALITY = {
    "螺纹钢": {"去库开始月": [3, 4, 5], "增库开始月": [11, 12, 1], "注意月": [2, 6]},
    "铁矿石": {"去库开始月": [3, 4, 5], "增库开始月": [11, 12, 1], "注意月": [7, 8]},
    "铜": {"去库开始月": [4, 5, 6], "增库开始月": [10, 11], "注意月": [3, 9]},
    "铝": {"去库开始月": [4, 5, 6], "增库开始月": [10, 11], "注意月": [3, 9]},
    "甲醇": {"去库开始月": [3, 4, 9], "增库开始月": [6, 7, 8, 11], "注意月": [2]},
    "豆粕": {"去库开始月": [10, 11], "增库开始月": [3, 4, 5], "注意月": [6, 7]},
    "豆油": {"去库开始月": [10, 11], "增库开始月": [3, 4, 5], "注意月": [1, 9]},
    "棕榈油": {"去库开始月": [11, 12], "增库开始月": [3, 4, 5], "注意月": [7, 8]},
    "白糖": {"去库开始月": [4, 5, 6], "增库开始月": [11, 12], "注意月": [2, 9]},
    "棉花": {"去库开始月": [3, 4, 5], "增库开始月": [10, 11], "注意月": [8, 9]},
    "PTA": {"去库开始月": [3, 4, 11], "增库开始月": [6, 7, 8], "注意月": [1, 12]},
}

PRICE_LEAD_MAP = {
    "螺纹钢": {"领先价格": True, "周期": "1-2周", "说明": "库存降则未来价格上涨"},
    "铁矿石": {"领先价格": True, "周期": "1-2周", "说明": "港口库存降则矿价涨"},
    "铜": {"领先价格": True, "周期": "2-4周", "说明": "库存降则铜价涨"},
    "铝": {"领先价格": True, "周期": "1-2周", "说明": "库存降则铝价获支撑"},
    "甲醇": {"领先价格": True, "周期": "1周", "说明": "库存去化则甲醇企稳"},
    "棕榈油": {"领先价格": True, "周期": "2-3周", "说明": "库存降则油脂反弹"},
}


# ============================================================
# 工具函数
# ============================================================

def _parse_number(val) -> Optional[float]:
    """解析库存数字字符串，返回浮点数（万吨）"""
    if val is None or val in ["N/A", "None", "-", "暂无数据", ""]:
        return None
    s = str(val).strip().replace(",", "").replace("，", "").replace(" ", "")
    multiplier = 1.0
    if "万" in s:
        s = s.replace("万", "")
    if "亿" in s:
        s = s.replace("亿", "")
        multiplier = 10000.0
    try:
        return float(s) * multiplier
    except (ValueError, TypeError):
        return None


def _analyze_trend(values: List[float]) -> dict:
    """分析库存变化趋势"""
    if len(values) < 2:
        return {"trend": "数据不足", "change_pct": 0.0, "signal": "无信号", "momentum_pct": 0.0}

    valid = [v for v in values if v is not None]
    if len(valid) < 2:
        return {"trend": "数据不足", "change_pct": 0.0, "signal": "无信号", "momentum_pct": 0.0}

    latest = valid[0]
    prev = valid[-1]
    change_pct = ((latest - prev) / prev * 100) if prev != 0 else 0.0

    if change_pct < -5:
        trend = "快速去库"
        signal = "利好价格"
    elif -5 <= change_pct < 0:
        trend = "缓慢去库"
        signal = "支撑价格"
    elif 0 <= change_pct <= 1:
        trend = "库存平稳"
        signal = "中性"
    elif 1 < change_pct <= 5:
        trend = "缓慢增库"
        signal = "压制价格"
    else:
        trend = "快速增库"
        signal = "利空价格"

    # 近3期均值动量
    if len(valid) >= 3:
        avg_recent = sum(valid[:3]) / 3
        avg_prev = sum(valid[3:6]) / min(3, len(valid) - 3) if len(valid) > 3 else avg_recent
        momentum = ((avg_recent - avg_prev) / avg_prev * 100) if avg_prev != 0 else 0.0
    else:
        momentum = 0.0

    return {
        "trend": trend,
        "change_pct": round(change_pct, 2),
        "signal": signal,
        "latest_value": round(latest, 2),
        "prev_value": round(prev, 2),
        "momentum_pct": round(momentum, 2),
    }


def _seasonal_note(product: str, change_pct: float) -> str:
    """根据季节性给出提示"""
    now = datetime.now()
    month = now.month
    cfg = INVENTORY_SEASONALITY.get(product, {})
    if not cfg:
        return ""

    drain_months = cfg.get("去库开始月", [])
    build_months = cfg.get("增库开始月", [])
    watch_months = cfg.get("注意月", [])

    if month in drain_months:
        if change_pct < 0:
            return f"[{product}：当前季节性去库中，库存下降符合预期]"
        else:
            return f"[{product}：季节性去库期，但库存反增，需警惕]"
    elif month in build_months:
        if change_pct > 0:
            return f"[{product}：当前季节性增库中，库存增加符合预期]"
        else:
            return f"[{product}：季节性增库期，但库存反降，关注]"
    elif month in watch_months:
        return f"[{product}：库存周期转换期，需密切关注]"
    return ""


def _fetch_series(symbol_name: str, max_records: int = 12) -> list:
    """获取历史库存序列（最新在前），同时返回日期信息"""
    try:
        time.sleep(0.5)
        df = ak.futures_inventory_em(symbol=symbol_name)
        if df is None or len(df) == 0:
            return []
        val_col = None
        date_col = None
        for col in df.columns:
            c = str(col).lower()
            if any(x in c for x in ["库存", "数量"]):
                val_col = col
            if any(x in c for x in ["日期", "date", "时间"]):
                date_col = col
        if val_col is None and len(df.columns) >= 2:
            val_col = df.columns[1]
        if date_col is None and len(df.columns) >= 1:
            date_col = df.columns[0]
        if val_col is None:
            return []
        values = []
        for i in range(min(max_records, len(df))):
            raw = df.iloc[i].get(val_col)
            parsed = _parse_number(raw)
            if parsed is not None:
                values.append(parsed)
        return values
    except Exception:
        return []


def _fetch_series_with_date(symbol_name: str, max_records: int = 12) -> dict:
    """获取历史库存序列（最新在前），同时返回最新日期"""
    try:
        time.sleep(0.5)
        df = ak.futures_inventory_em(symbol=symbol_name)
        if df is None or len(df) == 0:
            return {"values": [], "latest_date": None}

        val_col = None
        date_col = None
        for col in df.columns:
            c = str(col).lower()
            if any(x in c for x in ["库存", "数量"]):
                val_col = col
            if any(x in c for x in ["日期", "date", "时间"]):
                date_col = col
        if val_col is None and len(df.columns) >= 2:
            val_col = df.columns[1]
        if date_col is None and len(df.columns) >= 1:
            date_col = df.columns[0]
        if val_col is None:
            return {"values": [], "latest_date": None}

        values = []
        latest_date = None

        for i in range(min(max_records, len(df))):
            row = df.iloc[i]
            raw = row.get(val_col)
            parsed = _parse_number(raw)
            if parsed is not None:
                values.append(parsed)
                # 获取最新日期（第一行）
                if i == 0 and date_col:
                    date_raw = row.get(date_col, "")
                    latest_date = str(date_raw)[:10] if date_raw else None

        return {"values": values, "latest_date": latest_date}
    except Exception:
        return {"values": [], "latest_date": None}


def _check_data_freshness(date_str: str) -> dict:
    """
    检查库存数据时效性
    返回: {"days_old": int, "status": str, "label": str}
    """
    if not date_str:
        return {"days_old": None, "status": "unknown", "label": "⚠️ 日期未知"}

    try:
        data_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        days_old = (today - data_date).days

        if days_old <= 3:
            return {"days_old": days_old, "status": "fresh", "label": f"✅ 最新({date_str})"}
        elif days_old <= 7:
            return {"days_old": days_old, "status": "stale", "label": f"⚠️ 数据过期({date_str}, {days_old}天前)"}
        else:
            return {"days_old": days_old, "status": "unreliable", "label": f"❌ 数据不可靠({date_str}, {days_old}天前)"}
    except Exception:
        return {"days_old": None, "status": "unknown", "label": f"⚠️ 日期格式异常({date_str})"}


# ============================================================
# 主函数
# ============================================================

def get_inventory_trend_analysis() -> dict:
    """
    获取全品种库存环比趋势分析
    返回结构化字典，集成到 all_data["inventory_trend"]
    """
    print("\n📊 库存环比趋势分析...")
    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "products": {},
        "signals": [],
        "divergences": [],
        "summary": ""
    }

    for product, symbol in INVENTORY_PRODUCTS.items():
        series_data = _fetch_series_with_date(symbol, max_records=12)
        values = series_data["values"]
        latest_date = series_data["latest_date"]

        trend_info = _analyze_trend(values)
        change_pct = trend_info["change_pct"]
        seasonal = _seasonal_note(product, change_pct)
        lead = PRICE_LEAD_MAP.get(product, {})

        # 时效性检查
        freshness = _check_data_freshness(latest_date)

        latest_str = f"{trend_info['latest_value']:.2f}" if values else "N/A"
        change_str = "下降" if change_pct < -1 else ("上升" if change_pct > 1 else "持平")

        result["products"][product] = {
            "库存": latest_str,
            "变化文字": change_str,
            "周环比_pct": trend_info["change_pct"],
            "趋势": trend_info["trend"],
            "信号": trend_info["signal"],
            "动量_pct": trend_info["momentum_pct"],
            "季节性提示": seasonal,
            "库存价格领先关系": lead,
            # 新增：时效性信息
            "数据日期": latest_date or "未知",
            "时效状态": freshness["status"],
            "时效标签": freshness["label"],
            "数据天龄": freshness["days_old"],
        }

        # 生成信号列表
        if trend_info["trend"] in ["快速去库", "缓慢去库"]:
            result["signals"].append({
                "product": product,
                "type": "去库",
                "trend": trend_info["trend"],
                "change_pct": trend_info["change_pct"],
                "signal": trend_info["signal"],
                "seasonal": seasonal,
                "level": "high" if "快速" in trend_info["trend"] else "medium",
                "data_date": latest_date,
                "freshness_label": freshness["label"],
            })
        elif trend_info["trend"] in ["快速增库", "缓慢增库"]:
            result["signals"].append({
                "product": product,
                "type": "增库",
                "trend": trend_info["trend"],
                "change_pct": trend_info["change_pct"],
                "signal": trend_info["signal"],
                "seasonal": seasonal,
                "level": "high" if "快速" in trend_info["trend"] else "medium",
                "data_date": latest_date,
                "freshness_label": freshness["label"],
            })

    drain = [s for s in result["signals"] if s["type"] == "去库"]
    build = [s for s in result["signals"] if s["type"] == "增库"]
    valid_cnt = sum(1 for v in result["products"].values() if v["趋势"] != "数据不足")

    # 统计时效性
    stale_count = sum(1 for v in result["products"].values()
                      if v["时效状态"] in ["stale", "unreliable"])
    unreliable_count = sum(1 for v in result["products"].values()
                           if v["时效状态"] == "unreliable")

    result["summary"] = (
        f"库存分析：{len(result['products'])}个品种，"
        f"去库{len(drain)}个，增库{len(build)}个。"
        f"螺纹钢/铁矿石关注港口库存，"
        f"油脂类关注季节性去库节奏。"
    )
    if stale_count > 0:
        result["summary"] += f" ⚠️ {stale_count}个品种数据过期"
    if unreliable_count > 0:
        result["summary"] += f"，❌ {unreliable_count}个品种数据不可靠"

    print(f"  ✅ 分析完成：{valid_cnt}/{len(result['products'])} 个品种")
    if result["signals"]:
        for s in result["signals"][:6]:
            freshness_str = s.get("freshness_label", "")
            print(f"    {s['product']}: {s['trend']} ({s['change_pct']:+.1f}%) -> {s['signal']} {freshness_str}")

    return result


def enrich_with_price_divergence(trend_data: dict, category_data: dict) -> dict:
    """
    库存-价格背离分析
    价格涨+库存增 = 顶部背离；价格跌+库存去 = 底部背离
    """
    divergences = []
    products = trend_data.get("products", {})

    for product, inv in products.items():
        if inv.get("周环比_pct") is None or inv.get("周环比_pct") == 0:
            continue
        # 找对应行情
        price_info = None
        for sym, info in category_data.items():
            if info.get("product") == product or product in str(info.get("product", "")):
                price_info = info
                break
        if not price_info or price_info.get("status") != "OK":
            continue

        price_chg = price_info.get("change_pct", 0)
        inv_chg = inv.get("周环比_pct", 0)

        if price_chg > 1 and inv_chg > 1:
            divergences.append({
                "product": product,
                "type": "顶部背离",
                "price_change": price_chg,
                "inventory_change": inv_chg,
                "signal": f"{product}价格强势但库存在增，警惕见顶",
                "level": "high"
            })
        elif price_chg < -1 and inv_chg < -1:
            divergences.append({
                "product": product,
                "type": "底部背离",
                "price_change": price_chg,
                "inventory_change": inv_chg,
                "signal": f"{product}价格下跌但库存在去，关注超跌反弹",
                "level": "medium"
            })

    trend_data["divergences"] = divergences
    if divergences:
        div_str = "; ".join([f"{d['product']}({d['type']})" for d in divergences[:3]])
        trend_data["summary"] = trend_data.get("summary", "") + f" 背离: {div_str}"
    return trend_data


def format_inventory_text(trend_data: dict) -> str:
    """格式化库存趋势为文本（含时效性标注）"""
    lines = ["\n📦 库存环比趋势分析", "=" * 45]
    signals = trend_data.get("signals", [])
    divergences = trend_data.get("divergences", [])
    drain = [s for s in signals if s["type"] == "去库"]
    build = [s for s in signals if s["type"] == "增库"]

    if drain:
        lines.append("\n🔻 去库品种：")
        for s in drain[:5]:
            seasonal = s.get("seasonal", "")
            freshness = s.get("freshness_label", "")
            lines.append(f"  {s['product']}: {s['trend']} ({s['change_pct']:+.1f}%) {s['signal']} {freshness}")
            if seasonal:
                lines.append(f"    {seasonal}")
    if build:
        lines.append("\n🔺 增库品种：")
        for s in build[:5]:
            seasonal = s.get("seasonal", "")
            freshness = s.get("freshness_label", "")
            lines.append(f"  {s['product']}: {s['trend']} ({s['change_pct']:+.1f}%) {s['signal']} {freshness}")
            if seasonal:
                lines.append(f"    {seasonal}")

    # 显示所有品种的数据日期
    products = trend_data.get("products", {})
    if products:
        lines.append("\n📅 库存数据时效：")
        for product, info in list(products.items())[:10]:
            freshness_label = info.get("时效标签", "")
            lines.append(f"  {product}: {freshness_label}")

    if divergences:
        lines.append("\n⚠ 库存-价格背离信号：")
        for d in divergences[:3]:
            lines.append(f"  {d['product']}: {d['signal']}")
    return "\n".join(lines)


# ============================================================
# 测试
# ============================================================
if __name__ == "__main__":
    trend = get_inventory_trend_analysis()
    print(format_inventory_text(trend))
