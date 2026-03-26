# ==================== 经济数据日历模块 v1.0 ====================
"""
下周重要经济数据日历
数据源：akshare + 新闻聚合
返回结构化数据供 AI 分析使用
"""

import time
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
from typing import Optional


# ============================================================
# 高优先级事件配置（对期货影响最大的）
# ============================================================

HIGH_PRIORITY_EVENTS = [
    {"country": "美国", "name": "非农就业人口", "impact": "high", "frequency": "月"},
    {"country": "美国", "name": "失业率", "impact": "high", "frequency": "月"},
    {"country": "美国", "name": "CPI", "impact": "high", "frequency": "月"},
    {"country": "美国", "name": "PPI", "impact": "medium", "frequency": "月"},
    {"country": "美国", "name": "零售销售", "impact": "high", "frequency": "月"},
    {"country": "美国", "name": "ISM制造业PMI", "impact": "high", "frequency": "月"},
    {"country": "美国", "name": "PCE物价指数", "impact": "high", "frequency": "月"},
    {"country": "美国", "name": "核心PCE", "impact": "high", "frequency": "月"},
    {"country": "美国", "name": "GDP", "impact": "high", "frequency": "季"},
    {"country": "美国", "name": "美联储利率决议", "impact": "high", "frequency": "次"},
    {"country": "美国", "name": "鲍威尔讲话", "impact": "high", "frequency": "不定"},
    {"country": "美国", "name": "初请失业金人数", "impact": "medium", "frequency": "周"},
    {"country": "美国", "name": "耐用品订单", "impact": "medium", "frequency": "月"},
    {"country": "中国", "name": "CPI", "impact": "high", "frequency": "月"},
    {"country": "中国", "name": "PPI", "impact": "high", "frequency": "月"},
    {"country": "中国", "name": "GDP", "impact": "high", "frequency": "季"},
    {"country": "中国", "name": "官方制造业PMI", "impact": "high", "frequency": "月"},
    {"country": "中国", "name": "财新制造业PMI", "impact": "medium", "frequency": "月"},
    {"country": "中国", "name": "社会融资规模", "impact": "high", "frequency": "月"},
    {"country": "中国", "name": "贸易帐", "impact": "high", "frequency": "月"},
    {"country": "中国", "name": "LPR利率", "impact": "high", "frequency": "月"},
    {"country": "中国", "name": "MLF操作", "impact": "high", "frequency": "月"},
    {"country": "欧元区", "name": "CPI", "impact": "high", "frequency": "月"},
    {"country": "欧元区", "name": "ECB利率决议", "impact": "high", "frequency": "次"},
    {"country": "全球", "name": "OPEC+会议", "impact": "high", "frequency": "不定"},
]

# 关键词 -> 影响品种映射
EVENT_IMPACT_MAP = {
    "非农": ["铜", "金", "银", "原油"],
    "失业率": ["铜", "金", "银"],
    "CPI": ["黄金", "白银", "原油", "所有商品"],
    "PPI": ["螺纹钢", "铁矿石", "焦煤", "化工"],
    "零售销售": ["铜", "原油", "农产品"],
    "制造业PMI": ["螺纹钢", "铜", "铝"],
    "GDP": ["所有品种"],
    "美联储": ["黄金", "白银", "铜", "原油", "美元指数"],
    "利率决议": ["黄金", "白银", "铜", "原油"],
    "鲍威尔": ["黄金", "白银", "铜", "原油"],
    "LPR": ["螺纹钢", "铁矿石", "铜", "铝"],
    "MLF": ["螺纹钢", "铁矿石", "铜"],
    "ECB": ["黄金", "白银", "欧元"],
    "OPEC": ["原油", "燃料油", "沥青", "甲醇"],
    "贸易帐": ["大豆", "玉米", "棉花", "铜"],
    "出口": ["螺纹钢", "热卷", "铝"],
    "进口": ["铜", "大豆", "铁矿石", "原油"],
    "社会融资": ["螺纹钢", "铜", "铝"],
}


def _get_next_week_range() -> tuple:
    """获取下周一的日期和下下周日的日期"""
    today = datetime.now()
    days_until_monday = (7 - today.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    next_monday = today + timedelta(days=days_until_monday)
    next_sunday = next_monday + timedelta(days=6)
    return next_monday, next_sunday


def _fetch_calendar_akshare() -> pd.DataFrame:
    """通过akshare获取财经日历数据"""
    try:
        time.sleep(0.5)
        df = ak.macro_china_market_calendar()
        if df is not None and len(df) > 2:
            return df
    except Exception:
        pass
    try:
        time.sleep(0.5)
        df = ak.macro_calendar()
        if df is not None and len(df) > 2:
            return df
    except Exception:
        pass
    return pd.DataFrame()


def _fetch_news_events() -> list:
    """从新闻中提取重要数据事件预告"""
    events = []
    try:
        time.sleep(0.5)
        df = ak.stock_news_em()
        if df is None or len(df) == 0:
            return events
        keywords = [
            "非农", "CPI", "PPI", "美联储", "FOMC", "利率决议",
            "GDP", "PMI", "央行", "OPEC", "EIA", "USDA",
            "库存", "出口", "进口", "关税", "制裁", "降息", "加息",
            "LPR", "MLF", "议息"
        ]
        seen = set()
        for _, row in df.head(80).iterrows():
            title = str(row.get("新闻标题", ""))
            pub_time = str(row.get("发布时间", ""))[:16]
            for kw in keywords:
                if kw in title and title not in seen:
                    seen.add(title)
                    high_kws = ["美联储", "非农", "CPI", "利率", "OPEC", "降息", "加息", "FOMC"]
                    impact = "high" if kw in high_kws else "medium"
                    events.append({
                        "event": title[:70],
                        "source": str(row.get("媒体名称", "财经"))[:20],
                        "time": pub_time,
                        "impact": impact
                    })
                    if len(events) >= 8:
                        break
            if len(events) >= 8:
                break
    except Exception:
        pass
    return events


def _parse_calendar_df(df: pd.DataFrame) -> list:
    """解析akshare日历DataFrame，提取重要事件"""
    results = []
    if df is None or df.empty:
        return results
    col_map = {}
    for col in df.columns:
        c = str(col).strip().lower()
        if any(x in c for x in ["date", "日期", "时间"]):
            col_map["date"] = col
        elif any(x in c for x in ["name", "名称", "事件"]):
            col_map["name"] = col
        elif any(x in c for x in ["country", "国家", "地区"]):
            col_map["country"] = col
    if "date" not in col_map or "name" not in col_map:
        if len(df.columns) >= 2:
            col_map["date"] = df.columns[0]
            col_map["name"] = df.columns[1]
    date_col = col_map.get("date", "")
    name_col = col_map.get("name", "")
    country_col = col_map.get("country", "")
    priority_kws = [
        "非农", "CPI", "PPI", "PMI", "GDP", "利率", "美联储", "央行",
        "OPEC", "零售", "就业", "通胀", "LPR", "MLF", "ECB",
        "鲍威尔", "贸易", "EIA", "USDA", "FOMC", "议息", "关税"
    ]
    seen_names = set()
    for _, row in df.iterrows():
        if len(results) >= 10:
            break
        name = str(row.get(name_col, row.get(1, ""))).strip()
        if not name or name in seen_names:
            continue
        if not any(kw in name for kw in priority_kws):
            continue
        seen_names.add(name)
        date_val = str(row.get(date_col, row.get(0, "")))[:10]
        country = str(row.get(country_col, "全球")) if country_col else "全球"
        high_impact_kws = ["非农", "CPI", "利率决议", "GDP", "美联储", "OPEC", "央行", "FOMC"]
        impact = "high" if any(k in name for k in high_impact_kws) else "medium"
        affected = []
        for kw, products in EVENT_IMPACT_MAP.items():
            if kw in name:
                affected = products
                break
        results.append({
            "event": name[:60],
            "date": date_val,
            "country": country[:20],
            "impact": impact,
            "affected": affected[:3]
        })
    return results


def get_economic_calendar() -> dict:
    """
    获取下周重要经济数据日历，返回结构化字典
    可直接集成到 all_data["economic_calendar"]
    """
    print("\n📅 获取下周经济数据日历...")
    next_mon, next_sun = _get_next_week_range()
    week_range = f"{next_mon.strftime('%m/%d')} - {next_sun.strftime('%m/%d')}"

    result = {
        "week_range": week_range,
        "next_monday": next_mon.strftime("%Y-%m-%d"),
        "next_sunday": next_sun.strftime("%Y-%m-%d"),
        "high_impact_events": [],
        "medium_impact_events": [],
        "news_events": [],
        "affected_products": {},
        "summary": ""
    }

    # 1. akshare真实日历数据
    df = _fetch_calendar_akshare()
    if not df.empty:
        parsed = _parse_calendar_df(df)
        for ev in parsed:
            if ev["impact"] == "high":
                result["high_impact_events"].append(ev)
            else:
                result["medium_impact_events"].append(ev)

    # 2. 新闻事件预告
    result["news_events"] = _fetch_news_events()

    # 3. 补充配置（保证最少有数据可用）
    if len(result["high_impact_events"]) < 2:
        for ev in HIGH_PRIORITY_EVENTS[:5]:
            result["high_impact_events"].append({
                "event": f"{ev['country']} {ev['name']}",
                "country": ev["country"],
                "impact": ev["impact"],
                "frequency": ev["frequency"],
                "affected": EVENT_IMPACT_MAP.get(ev["name"], ["所有品种"])[:3],
                "note": "请关注具体发布时间"
            })

    # 4. 品种影响统计
    affected_count = {}
    for ev_list in [result["high_impact_events"], result["medium_impact_events"]]:
        for ev in ev_list:
            for prod in ev.get("affected", []):
                affected_count[prod] = affected_count.get(prod, 0) + 1
    result["affected_products"] = dict(
        sorted(affected_count.items(), key=lambda x: x[1], reverse=True)[:8]
    )

    high_count = len(result["high_impact_events"])
    result["summary"] = f"下周重要数据{high_count}个，关注美联储/央行动态及CPI对商品的影响"

    # 5. 去重
    seen_ev = set()
    deduped = []
    for ev in result["high_impact_events"]:
        key = ev["event"][:20]
        if key not in seen_ev:
            seen_ev.add(key)
            deduped.append(ev)
    result["high_impact_events"] = deduped[:8]

    # 打印摘要
    if result["high_impact_events"]:
        print(f"  ✅ 高影响数据: {len(result['high_impact_events'])} 个")
        for ev in result["high_impact_events"][:4]:
            print(f"    ⚠️ {ev.get('event', '')[:40]}")
    if result["news_events"]:
        print(f"  ✅ 数据相关新闻: {len(result['news_events'])} 条")
    if not result["high_impact_events"] and not result["news_events"]:
        print("  ⚠️ 暂无重大数据安排，关注日常发布")

    return result


def format_calendar_text(calendar: dict) -> str:
    """格式化日历为文本（供邮件/推送使用）"""
    lines = []
    week_range = calendar.get("week_range", "")
    lines.append(f"📅 下周经济数据日历 ({week_range})")
    lines.append("=" * 45)

    high = calendar.get("high_impact_events", [])
    medium = calendar.get("medium_impact_events", [])
    news_evs = calendar.get("news_events", [])
    affected = calendar.get("affected_products", {})

    if high:
        lines.append("\n🔶 高影响事件：")
        for ev in high[:5]:
            name = ev.get("event", "")
            lines.append(f"  ⚠ {name[:45]}")

    if medium:
        lines.append(f"\n🔷 中影响事件（{len(medium)} 个）：")
        for ev in medium[:3]:
            lines.append(f"  ？ {ev.get('event', '')[:45]}")

    if news_evs:
        lines.append("\n📰 数据相关新闻：")
        for ev in news_evs[:3]:
            lines.append(f"  📢 {ev.get('event', '')[:50]}")

    if affected:
        top_products = ", ".join(list(affected.keys())[:5])
        lines.append(f"\n📦 重点关注品种：{top_products}")

    return "\n".join(lines)


# ============================================================
# 测试入口
# ============================================================
if __name__ == "__main__":
    cal = get_economic_calendar()
    print("\n" + format_calendar_text(cal))
