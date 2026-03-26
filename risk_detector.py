# ==================== 地缘政治风险识别模块 v1.0 ====================
"""
地缘政治风险标签 + 新闻风险评分
- 从新闻中自动识别地缘政治/贸易摩擦等风险关键词
- 自动标记"美国"、"中国"、"欧盟"、"OPEC"、"关税"、"制裁"等关键词
- 返回结构化风险标签和评分
- 可直接集成到 all_data["risk_data"]
"""

import time
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
from typing import Optional, Dict, List


# ============================================================
# 风险关键词配置
# ============================================================

# 地缘政治核心关键词（最高优先级）
GEOPOLITICAL_KWS = [
    # 直接地缘冲突
    "战争", "冲突", "军事", "制裁", "禁运", "封锁", "对峙",
    "俄乌", "中东", "红海", "胡塞", "以色列", "巴勒斯坦", "加沙",
    "北约", "伊朗", "朝鲜", "台海", "南海", "东海",
    "核", "导弹", "无人机", "击落", "武装",
    # 贸易摩擦
    "关税", "贸易战", "加征关税", "取消关税", "贸易壁垒",
    "301调查", "337调查", "反补贴", "反倾销",
    # 制裁
    "制裁", "实体清单", "出口管制", "黑名单",
    "SDN制裁", "金融制裁", "能源制裁",
    # 重要资源博弈
    "OPEC+", "OPEC", "原油减产", "原油增产", "能源危机",
    "天然气", "LNG", "核能", "页岩油",
    # 大国关系
    "中美", "中欧", "欧美", "G7", "G20",
    "脱钩", "去风险", "供应链", "产业链转移",
]

# 贸易摩擦关键词
TRADE_FRICTION_KWS = [
    "关税", "贸易战", "加征", "取消", "豁免",
    "301", "337", "反补贴", "反倾销",
    "进口限制", "出口限制", "禁运",
    "最惠国", "NAFTA", "USMCA", "RCEP",
    "产业链", "供应链", "脱钩",
    "大豆", "玉米", "小麦", "猪肉", "棉花"  # 农业商品贸易摩擦
]

# 市场风险关键词
MARKET_RISK_KWS = [
    "暴跌", "暴涨", "闪崩", "熔断", "暂停交易",
    "违约", "破产", "债务危机", "流动性危机",
    "踩踏", "恐慌", "抛售", "做空",
    "银行危机", "金融危机", "经济衰退",
    "挤兑", "降级", "评级下调",
]

# 各品种对应风险关键词
PRODUCT_RISK_MAP = {
    "原油": ["OPEC", "制裁", "禁运", "中东", "俄乌", "能源", "页岩油", "原油减产", "原油增产", "红海", "胡塞"],
    "燃料油": ["OPEC", "制裁", "能源", "原油", "炼厂"],
    "沥青": ["OPEC", "原油", "道路", "基建"],
    "天然气": ["天然气", "LNG", "管道", "能源危机", "北溪", "制裁"],
    "铜": ["智利", "秘鲁", "罢工", "供应中断", "铜矿", "中美", "关税", "产业链", "脱钩", "新能源"],
    "铝": ["铝土矿", "氧化铝", "制裁", "俄铝", "电解铝", "中美", "关税", "新能源"],
    "锌": ["锌矿", "罢工", "供应", "镀锌"],
    "镍": ["印尼", "镍矿", "出口禁令", "不锈钢", "新能源", "电动汽车"],
    "螺纹钢": ["铁矿石", "巴西", "澳大利亚", "四大矿", "淡水河谷", "必和必拓", "力拓", "FMG",
              "环保", "限产", "碳中和", "房地产", "基建", "螺纹钢"],
    "铁矿石": ["淡水河谷", "必和必拓", "力拓", "FMG", "澳大利亚", "巴西", "四大矿",
              "港口", "发运", "库存", "海运费"],
    "焦煤": ["蒙古", "澳洲焦煤", "进口", "通关", "煤炭"],
    "焦炭": ["焦煤", "限产", "环保", "钢铁"],
    "大豆": ["USDA", "大豆", "南美", "巴西", "阿根廷", "美豆", "种植面积",
            "天气", "厄尔尼诺", "拉尼娜", "出口", "中国进口", "榨利"],
    "豆粕": ["大豆", "豆油", "压榨", "养殖", "饲料", "USDA", "南美"],
    "豆油": ["大豆", "棕榈油", "油脂", "生物柴油", "EPA", "RVO", "印尼", "马来西亚"],
    "棕榈油": ["印尼", "马来西亚", "厄尔尼诺", "拉尼娜", "产量", "出口", "油脂"],
    "玉米": ["USDA", "玉米", "种植面积", "南美", "巴西", "乌克兰", "出口",
            "天气", "厄尔尼诺", "饲料", "乙醇"],
    "白糖": ["巴西", "印度", "泰国", "白糖", "出口", "产量", "乙醇", "补贴"],
    "棉花": ["USDA", "美棉", "新疆棉", "印度棉", "巴西棉", "出口", "纺织", "中美"],
    "黄金": ["美联储", "利率", "黄金", "避险", "美元", "地缘", "央行购金", "实际利率", "通胀"],
    "白银": ["黄金", "贵金属", "工业需求", "避险", "金银比"],
    "黄金白银": ["美联储", "利率", "避险", "美元", "地缘", "央行购金"],
    "美元指数": ["美联储", "利率", "美元", "欧元", "日元", "非农", "CPI"],
}

# 风险等级权重
RISK_WEIGHTS = {
    "geopolitical": 3.0,    # 地缘政治最高
    "trade_friction": 2.5,  # 贸易摩擦次高
    "market_risk": 2.0,     # 市场风险
    "normal": 1.0,          # 正常
}


# ============================================================
# 核心函数
# ============================================================

def _classify_risk_level(keywords_found: List[str], all_kws: List[str]) -> str:
    """根据匹配到的关键词判断风险等级"""
    geo_found = [k for k in keywords_found if k in GEOPOLITICAL_KWS]
    trade_found = [k for k in keywords_found if k in TRADE_FRICTION_KWS]
    market_found = [k for k in keywords_found if k in MARKET_RISK_KWS]

    if geo_found:
        return "high"
    elif trade_found:
        return "high"
    elif market_found:
        return "medium"
    return "low"


def _extract_keywords(text: str, kw_list: List[str]) -> List[str]:
    """从文本中提取匹配到的关键词"""
    found = []
    for kw in kw_list:
        if kw in text:
            found.append(kw)
    return list(dict.fromkeys(found))  # 去重保持顺序


def _score_news_risk(title: str, all_kws: List[str]) -> dict:
    """评估单条新闻的风险分"""
    keywords_found = _extract_keywords(title, all_kws)
    level = _classify_risk_level(keywords_found, all_kws)

    # 风险分：基础分 + 关键词加分
    base_score = {"high": 80, "medium": 50, "low": 20}.get(level, 20)
    kw_bonus = min(len(keywords_found) * 5, 20)  # 每多一个关键词+5分，最高+20

    return {
        "risk_level": level,
        "risk_score": min(base_score + kw_bonus, 100),
        "keywords_found": keywords_found,
        "keyword_count": len(keywords_found),
    }


def _fetch_news_for_risk() -> list:
    """获取财经新闻用于风险识别"""
    try:
        time.sleep(0.5)
        df = ak.stock_news_em()
        if df is None or len(df) == 0:
            return []
        results = []
        for _, row in df.head(50).iterrows():
            title = str(row.get("新闻标题", ""))
            pub_time = str(row.get("发布时间", ""))[:16]
            source = str(row.get("媒体名称", ""))[:20]
            if title and len(title) > 5:
                results.append({
                    "title": title,
                    "time": pub_time,
                    "source": source,
                })
        return results
    except Exception:
        return []


def detect_risks(news_list: list = None) -> dict:
    """
    识别新闻中的地缘政治和贸易摩擦风险
    返回结构化风险数据，直接集成到 all_data["risk_data"]
    """
    print("\n🚨 地缘政治风险识别...")

    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "risk_level": "low",      # overall: low/medium/high/critical
        "risk_score": 0,          # 0-100
        "high_risk_news": [],     # 高风险新闻
        "medium_risk_news": [],   # 中风险新闻
        "category_risks": {},     # 各品种风险标签
        "geopolitical_tags": [],  # 地缘政治标签
        "trade_tags": [],         # 贸易摩擦标签
        "market_alerts": [],      # 市场风险预警
        "summary": ""
    }

    all_kws = GEOPOLITICAL_KWS + TRADE_FRICTION_KWS + MARKET_RISK_KWS
    all_news = news_list if news_list else _fetch_news_for_risk()

    if not all_news:
        result["summary"] = "暂无新闻数据，无法评估风险"
        print("  ⚠️ 暂无新闻数据")
        return result

    high_risk = []
    medium_risk = []
    geo_tags = set()
    trade_tags = set()
    market_alerts = []

    for news in all_news:
        title = news.get("title", "")
        if not title:
            continue

        # 评分
        score_info = _score_news_risk(title, all_kws)
        level = score_info["risk_level"]
        keywords = score_info["keywords_found"]

        # 分类整理
        geo_kws = [k for k in keywords if k in GEOPOLITICAL_KWS]
        trade_kws = [k for k in keywords if k in TRADE_FRICTION_KWS]
        market_kws = [k for k in keywords if k in MARKET_RISK_KWS]

        news_entry = {
            "title": title[:80],
            "time": news.get("time", ""),
            "source": news.get("source", ""),
            "risk_score": score_info["risk_score"],
            "risk_level": level,
            "keywords": keywords,
        }

        if level == "high":
            high_risk.append(news_entry)
            geo_tags.update(geo_kws)
            trade_tags.update(trade_kws)
        elif level == "medium":
            medium_risk.append(news_entry)
            if geo_kws:
                geo_tags.update(geo_kws)
            if trade_kws:
                trade_tags.update(trade_kws)

        if market_kws:
            market_alerts.append(news_entry)

    result["high_risk_news"] = sorted(high_risk, key=lambda x: x["risk_score"], reverse=True)[:8]
    result["medium_risk_news"] = sorted(medium_risk, key=lambda x: x["risk_score"], reverse=True)[:6]
    result["geopolitical_tags"] = sorted(list(geo_tags))
    result["trade_tags"] = sorted(list(trade_tags))
    result["market_alerts"] = market_alerts[:5]

    # 品种风险标签
    category_risks = {}
    for product, relevant_kws in PRODUCT_RISK_MAP.items():
        matched = [k for k in relevant_kws if any(k in n.get("title", "") for n in all_news)]
        if matched:
            level = "high" if any(k in GEOPOLITICAL_KWS for k in matched) else "medium"
            category_risks[product] = {
                "level": level,
                "matched_keywords": matched[:5],
                "news_count": len(matched),
                "description": f"涉及：{'/'.join(matched[:3])}"
            }
    result["category_risks"] = category_risks

    # 综合风险等级
    if high_risk:
        max_score = max(h["risk_score"] for h in high_risk) if high_risk else 0
        result["risk_score"] = max_score
        if any(k in geo_tags for k in ["战争", "制裁", "禁运", "OPEC+", "原油减产", "原油增产"]):
            result["risk_level"] = "critical"
        elif any(k in trade_tags for k in ["关税", "贸易战", "加征", "制裁", "实体清单"]):
            result["risk_level"] = "high"
        else:
            result["risk_level"] = "high"
    elif medium_risk:
        result["risk_score"] = max((m["risk_score"] for m in medium_risk), default=0)
        result["risk_level"] = "medium"
    else:
        result["risk_score"] = 10
        result["risk_level"] = "low"

    # 摘要
    risk_desc = {
        "critical": "极高风险",
        "high": "高风险",
        "medium": "中等风险",
        "low": "低风险"
    }.get(result["risk_level"], "未知")

    geo_str = "、".join(list(geo_tags)[:5]) if geo_tags else "无"
    trade_str = "、".join(list(trade_tags)[:5]) if trade_tags else "无"
    result["summary"] = (
        f"风险等级{risk_desc}（{result['risk_score']}分），"
        f"高风险新闻{len(high_risk)}条。"
        f"地缘标签：{geo_str}。"
        f"贸易标签：{trade_str}。"
    )

    # 打印摘要
    print(f"  ✅ 风险等级: {risk_desc} ({result['risk_score']}分)")
    if result["geopolitical_tags"]:
        print(f"  🌍 地缘标签: {', '.join(result['geopolitical_tags'][:6])}")
    if result["trade_tags"]:
        print(f"  💼 贸易标签: {', '.join(result['trade_tags'][:6])}")
    if high_risk:
        print(f"  ⚠️ 高风险新闻 {len(high_risk)} 条:")
        for n in high_risk[:3]:
            print(f"    - {n['title'][:50]}")
    if not high_risk and not medium_risk:
        print("  ✅ 未发现明显地缘政治风险")

    return result


def tag_product_risks(risk_data: dict, category_data: dict) -> dict:
    """
    将风险标签应用到具体品种
    返回带风险标签的品种数据
    """
    category_risks = risk_data.get("category_risks", {})
    geo_tags = risk_data.get("geopolitical_tags", [])
    trade_tags = risk_data.get("trade_tags", [])

    tagged = {}
    for symbol, info in category_data.items():
        if info.get("status") != "OK":
            continue
        product = info.get("product", "")
        # 找匹配风险
        risk_info = category_risks.get(product, {})
        if not risk_info:
            # 模糊匹配
            for prod_name, r in category_risks.items():
                if prod_name in product or product in prod_name:
                    risk_info = r
                    break

        if risk_info:
            tagged[symbol] = {
                **info,
                "risk_tag": {
                    "level": risk_info["level"],
                    "keywords": risk_info["matched_keywords"],
                    "description": risk_info["description"],
                    "geo_active": any(k in geo_tags for k in GEOPOLITICAL_KWS),
                    "trade_active": any(k in trade_tags for k in TRADE_FRICTION_KWS),
                }
            }
        else:
            tagged[symbol] = {**info, "risk_tag": {"level": "low", "keywords": [], "description": "无明显风险"}}

    return tagged


def format_risk_text(risk_data: dict) -> str:
    """格式化风险数据为文本"""
    lines = ["\n🚨 地缘政治风险识别", "=" * 45]
    level_map = {"critical": "极高", "high": "高", "medium": "中", "low": "低"}
    level_str = level_map.get(risk_data.get("risk_level", ""), "未知")
    score = risk_data.get("risk_score", 0)

    lines.append(f"\n风险等级: {level_str} ({score}分)")

    geo = risk_data.get("geopolitical_tags", [])
    trade = risk_data.get("trade_tags", [])
    if geo:
        lines.append(f"\n🌍 地缘政治: {', '.join(geo[:6])}")
    if trade:
        lines.append(f"💼 贸易摩擦: {', '.join(trade[:6])}")

    high = risk_data.get("high_risk_news", [])
    medium = risk_data.get("medium_risk_news", [])
    if high:
        lines.append(f"\n⚠ 高风险新闻 ({len(high)}条)：")
        for n in high[:4]:
            kws = ", ".join(n.get("keywords", [])[:3])
            lines.append(f"  [{n['risk_score']}分] {n['title'][:50]}")
            if kws:
                lines.append(f"    关键词: {kws}")
    if medium:
        lines.append(f"\n⚡ 中风险新闻 ({len(medium)}条)：")
        for n in medium[:3]:
            lines.append(f"  [{n['risk_score']}分] {n['title'][:50]}")

    alerts = risk_data.get("market_alerts", [])
    if alerts:
        lines.append(f"\n⚠ 市场预警 ({len(alerts)}条)：")
        for a in alerts[:3]:
            lines.append(f"  {a['title'][:55]}")

    return "\n".join(lines)


# ============================================================
# 测试
# ============================================================
if __name__ == "__main__":
    risks = detect_risks()
    print(format_risk_text(risks))
