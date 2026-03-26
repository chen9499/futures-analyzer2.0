# ==================== 数据采集模块 v6.0 ====================
"""
全维度数据采集 v6.0：稳定性优先，支持多接口自动降级
新增：
  - 多周期共振分析
  - 板块轮动规律预测
  - 风险评分与头寸管理
"""

import time
import pandas as pd
import akshare as ak
from datetime import datetime

# 第一阶段模块
from economic_calendar import get_economic_calendar, format_calendar_text
from inventory_trend import get_inventory_trend_analysis, enrich_with_price_divergence, format_inventory_text
from risk_detector import detect_risks, tag_product_risks, format_risk_text

# 第二阶段新增模块
from basis_analysis import analyze_basis, format_basis_text
from institutional_positions import get_positions_summary, format_positions_text
from signal_scoring import score_all_symbols, format_scores_text


# ============================================================
#  Part 1：行情数据（实时优先，多接口自动降级）
# ============================================================

# 期货代码 -> futures_zh_realtime 中文品种名映射
# 通过测试确认的正确名称
PRODUCT_REALTIME_NAME = {
    # 黑色系（大商所/上期所）
    "RB": "螺纹钢", "HC": "热轧卷板", "I": "铁矿石", "J": "焦炭", "JM": "焦煤",
    # 有色金属（上期所，需加"沪"前缀）
    "CU": "沪铜", "AL": "沪铝", "ZN": "沪锌", "NI": "沪镍", "SN": "沪锡", "PB": "沪铅",
    # 贵金属
    "AU": "黄金", "AG": "白银",
    # 能化
    "SC": "原油", "TA": "PTA", "MA": "甲醇", "FU": "燃料油", "BU": "沥青",
    "L": "塑料", "PP": "PP", "EG": "乙二醇",
    # 农产品
    "M": "豆粕", "Y": "豆油", "P": "棕榈", "C": "玉米", "SR": "白糖",
    "CF": "棉花", "AP": "鲜苹果", "RM": "菜粕", "OI": "菜油",
    # 金融期货
    "IF": "沪深300", "IC": "中证500", "IM": "中证1000", "IH": "上证50",
}


def _get_realtime_price(symbol: str) -> dict:
    """
    优先使用 futures_zh_realtime 获取实时行情
    返回 {"price": float, "volume": int, "open_interest": int, "date": str, "source": str}
    或 None
    """
    # 提取品种代码（去掉合约月份）
    product_code = ""
    for i, c in enumerate(symbol):
        if c.isdigit():
            product_code = symbol[:i].upper()
            break
    if not product_code:
        product_code = symbol.upper()

    cn_name = PRODUCT_REALTIME_NAME.get(product_code)
    if not cn_name:
        return None

    try:
        time.sleep(0.3)
        df = ak.futures_zh_realtime(symbol=cn_name)
        if df is None or len(df) == 0:
            return None

        # 找主力合约（symbol 以 0 结尾，如 RB0）或匹配合约月份
        target_row = None
        # 优先找精确匹配的合约
        for _, row in df.iterrows():
            row_sym = str(row.get('symbol', '')).upper()
            if row_sym == symbol.upper():
                target_row = row
                break
        # 其次找主力合约（symbol 末尾是 0）
        if target_row is None:
            for _, row in df.iterrows():
                row_sym = str(row.get('symbol', ''))
                if row_sym.endswith('0'):
                    target_row = row
                    break
        # 最后用第一行
        if target_row is None:
            target_row = df.iloc[0]

        trade = float(target_row.get('trade', 0) or 0)
        settlement = float(target_row.get('settlement', 0) or 0)
        presettlement = float(target_row.get('presettlement', 0) or 0)
        volume = int(target_row.get('volume', 0) or 0)
        position = int(target_row.get('position', 0) or 0)
        tradedate = str(target_row.get('tradedate', ''))

        # 实时价格优先用 trade，盘后用 settlement
        price = trade if trade > 0 else settlement
        if price <= 0:
            return None

        # 验证日期是否是今天
        today = datetime.now().strftime("%Y-%m-%d")
        date_ok = tradedate.startswith(today) if tradedate else False

        return {
            "price": price,
            "volume": volume,
            "open_interest": position,
            "date": tradedate,
            "date_ok": date_ok,
            "source": "realtime",
            "presettlement": presettlement,
        }
    except Exception:
        return None


def _fetch_daily(symbol: str, product: str):
    """
    采集日线数据，支持多接口自动降级（作为实时接口的备用）
    返回 DataFrame 或 None
    """
    # 策略1：东财历史K线（更稳定）
    try:
        time.sleep(0.5)
        df2 = ak.futures_hist_em(symbol=symbol, period="daily", start_date="20260101")
        if df2 is not None and len(df2) >= 5:
            return df2
    except Exception:
        pass

    # 策略2：东财按品种名
    try:
        time.sleep(0.5)
        df3 = ak.futures_hist_em(symbol=product, period="daily", start_date="20260101")
        if df3 is not None and len(df3) >= 5:
            return df3
    except Exception:
        pass

    # 策略3：新浪日线（网络可用时）
    try:
        time.sleep(0.5)
        df = ak.futures_zh_daily_sina(symbol=symbol)
        if df is not None and len(df) >= 5:
            return df
    except Exception:
        pass

    return None


def get_all_categories_data(categories: dict) -> dict:
    """采集全品种行情数据（实时优先，日线降级）"""
    results = {}

    for category, symbols in categories.items():
        print(f"\n📦 采集 [{category}]...")

        for symbol in symbols:
            if symbol[:2].isalpha():
                product = symbol[:2]
            else:
                product = symbol[:1]

            # ── 策略1：优先使用实时行情接口 ──────────────────
            rt = _get_realtime_price(symbol)
            if rt and rt["price"] > 0:
                price = rt["price"]
                volume = rt["volume"]
                oi = rt["open_interest"]
                presettlement = rt.get("presettlement", 0)
                # 计算涨跌幅（用昨结算价）
                change = ((price - presettlement) / presettlement * 100) if presettlement > 0 else 0

                results[symbol] = {
                    "category": category,
                    "product": product,
                    "price": price,
                    "prev_close": presettlement,
                    "change_pct": round(change, 2),
                    "volume": volume,
                    "open_interest": oi,
                    "status": "OK",
                    "data_source": "realtime",
                    "data_date": rt.get("date", ""),
                    "_df": None  # 实时接口无历史df，技术指标需另外获取
                }
                date_flag = "✅今日" if rt.get("date_ok") else "⚠️非今日"
                print(f"  ✅ {symbol}: {price} ({change:+.2f}%) [实时 {date_flag}]")
                continue

            # ── 策略2：降级到日线数据（用于技术指标计算）────
            df = _fetch_daily(symbol, product)

            if df is not None and len(df) >= 2:
                latest = df.iloc[-1]
                prev = df.iloc[-2]

                price = float(latest.get('收盘', latest.get('close',
                            latest.get('收盘价', latest.get('settlement', 0)))))
                prev_price = float(prev.get('收盘', prev.get('close',
                                prev.get('收盘价', prev.get('settlement', 0)))))
                change = ((price - prev_price) / prev_price * 100) if prev_price != 0 else 0
                volume = int(latest.get('成交量', latest.get('volume', 0)) or 0)
                oi = int(latest.get('持仓量', latest.get('open_interest',
                            latest.get('OI', latest.get('持仓', 0)) or 0)))

                results[symbol] = {
                    "category": category,
                    "product": product,
                    "price": price,
                    "prev_close": prev_price,
                    "change_pct": round(change, 2),
                    "volume": volume,
                    "open_interest": oi,
                    "status": "OK",
                    "data_source": "daily",
                    "data_date": str(latest.get('日期', latest.get('date', '')))[:10],
                    "_df": df  # 保存原始df供技术指标用
                }
                print(f"  ⚠️ {symbol}: {price} ({change:+.2f}%) [日线降级]")
            else:
                results[symbol] = {"category": category, "product": product, "status": "失败"}
                print(f"  ❌ {symbol}: 采集失败")

    return results


# ============================================================
#  Part 2：技术指标
# ============================================================

def _calc_indicators_from_df(df: pd.DataFrame) -> dict:
    """从已有DataFrame计算技术指标"""
    if df is None or len(df) < 20:
        return {}

    close = pd.to_numeric(df['收盘'] if '收盘' in df.columns else df.get('close', df.iloc[:, 0]),
                          errors='coerce').dropna()
    if len(close) < 20:
        return {}

    high_col = '最高' if '最高' in df.columns else 'high'
    low_col = '最低' if '最低' in df.columns else 'low'
    vol_col = '成交量' if '成交量' in df.columns else 'volume'

    high = pd.to_numeric(df[high_col], errors='coerce')
    low = pd.to_numeric(df[low_col], errors='coerce')
    vol = pd.to_numeric(df[vol_col], errors='coerce')

    if len(close) < 20:
        return {}

    ma5 = close.rolling(5).mean().iloc[-1]
    ma10 = close.rolling(10).mean().iloc[-1]
    ma20 = close.rolling(20).mean().iloc[-1]
    ma60 = close.rolling(60).mean().iloc[-1] if len(close) >= 60 else None

    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    rsi = (100 - 100 / (1 + rs)).iloc[-1]

    macd_s = close.ewm(span=12, adjust=False).mean() - close.ewm(span=26, adjust=False).mean()
    sig_s = macd_s.ewm(span=9, adjust=False).mean()
    macd_line = float(macd_s.iloc[-1])
    signal_line = float(sig_s.iloc[-1])
    macd_hist = macd_line - signal_line

    bb_mid = close.rolling(20).mean().iloc[-1]
    bb_std = close.rolling(20).std().iloc[-1]
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std
    cur_price = float(close.iloc[-1])
    bb_position = (cur_price - bb_lower) / (bb_upper - bb_lower) if bb_upper != bb_lower else 0.5

    ma5_s = close.rolling(5).mean()
    ma10_s = close.rolling(10).mean()
    golden_cross = bool((ma5_s.iloc[-1] > ma10_s.iloc[-1]) and (ma5_s.iloc[-2] <= ma10_s.iloc[-2]))
    dead_cross = bool((ma5_s.iloc[-1] < ma10_s.iloc[-1]) and (ma5_s.iloc[-2] >= ma10_s.iloc[-2]))

    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = float(tr.rolling(14).mean().iloc[-1])

    vol_avg5 = vol.iloc[-5:].mean()
    vol_ratio = float(vol.iloc[-1] / vol_avg5) if vol_avg5 > 0 else 1.0

    trend = "震荡"
    if cur_price > ma20 > ma10:
        trend = "多头"
    elif cur_price < ma20 < ma10:
        trend = "空头"

    sentiment = "中性"
    if rsi > 70:
        sentiment = "偏热"
    elif rsi < 30:
        sentiment = "偏冷"
    if golden_cross:
        sentiment = "买入信号"
    if dead_cross:
        sentiment = "卖出信号"

    return {
        "ma5": round(float(ma5), 2),
        "ma10": round(float(ma10), 2),
        "ma20": round(float(ma20), 2),
        "ma60": round(float(ma60), 2) if ma60 else None,
        "rsi": round(float(rsi), 1),
        "macd_line": round(macd_line, 2),
        "macd_signal": round(signal_line, 2),
        "macd_hist": round(macd_hist, 2),
        "bb_upper": round(bb_upper, 2),
        "bb_mid": round(bb_mid, 2),
        "bb_lower": round(bb_lower, 2),
        "bb_position": round(bb_position, 2),
        "atr": round(atr, 2),
        "golden_cross": golden_cross,
        "dead_cross": dead_cross,
        "trend": trend,
        "sentiment": sentiment,
        "volume_ratio": round(vol_ratio, 2),
        "price": cur_price,
    }


def enrich_with_indicators(category_data: dict) -> dict:
    """为品种补充技术指标（使用已有的df缓存，或重新获取历史数据）"""
    print(f"\n📈 计算技术指标...")
    count = 0
    for symbol, info in category_data.items():
        if info.get("status") != "OK":
            info["tech"] = {}
            continue
        df = info.pop("_df", None)  # 取出缓存的df

        # 如果是实时数据（无df），需要获取历史数据用于技术指标
        if df is None:
            product = info.get("product", "")
            try:
                time.sleep(0.3)
                df = ak.futures_hist_em(symbol=symbol, period="daily", start_date="20250101")
                if df is None or len(df) < 20:
                    df = ak.futures_hist_em(symbol=product, period="daily", start_date="20250101")
            except Exception:
                df = None

        if df is not None:
            try:
                info["tech"] = _calc_indicators_from_df(df)
                count += 1
            except Exception:
                info["tech"] = {}
        else:
            info["tech"] = {}
        time.sleep(0.2)
    print(f"✅ 技术指标完成（{count} 个品种）")
    return category_data


# ============================================================
#  Part 3：库存数据（东财）
# ============================================================

INVENTORY_PRODUCTS = {
    "黑色系": ["螺纹钢", "热轧卷板", "铁矿石", "焦炭", "焦煤"],
    "有色金属": ["铜", "铝", "锌", "镍", "锡"],
    "能化系": ["原油", "燃料油", "塑料", "聚丙烯", "甲醇", "PTA", "沥青"],
    "农产品": ["豆粕", "豆油", "棕榈油", "玉米", "白糖", "棉花", "苹果"],
}

INVENTORY_EXCHANGE = {
    "螺纹钢": "螺纹钢", "热轧卷板": "热轧卷板", "铁矿石": "铁矿石",
    "铜": "铜", "铝": "铝", "锌": "锌", "镍": "镍",
    "原油": "原油", "甲醇": "甲醇", "PTA": "PTA",
    "豆粕": "豆粕", "豆油": "豆油", "棕榈油": "棕榈油",
}


def get_inventory_data() -> dict:
    """获取主要品种库存数据"""
    inventories = {}
    print(f"\n📦 采集库存数据...")

    for name in INVENTORY_EXCHANGE:
        try:
            time.sleep(0.5)
            df = ak.futures_inventory_em(symbol=name)
            if df is not None and len(df) > 0:
                latest = df.iloc[0]
                inv_val = latest.get('库存', latest.get('库存量', 'N/A'))
                change_val = latest.get('增减', latest.get('变化', ''))
                inventories[name] = {
                    "库存": str(inv_val),
                    "变化": str(change_val),
                    "单位": "吨/手",
                    "日期": str(latest.get('日期', ''))[:10]
                }
                print(f"  ✅ {name}: 库存 {inv_val} ({change_val})")
        except Exception as e:
            pass

    if not inventories:
        print(f"  ⚠️ 库存数据暂不可用")
    return inventories


# ============================================================
#  Part 4：政策与宏观
# ============================================================

def get_policy_data() -> dict:
    """获取政策与宏观数据"""
    policies = {}
    print(f"\n📋 采集政策与宏观数据...")

    # 货币供应量
    try:
        time.sleep(0.5)
        df = ak.macro_china_money_supply()
        if df is not None and len(df) > 0:
            latest = df.iloc[0]
            m2 = str(latest.get("M2同比", latest.get("M2", "N/A")))[:8]
            m1 = str(latest.get("M1同比", latest.get("M1", "N/A")))[:8]
            policies["货币供应"] = {"M2同比": m2, "M1同比": m1,
                                   "时间": str(latest.get("月份", ""))[:10]}
            print(f"  ✅ 货币供应: M2 {m2}, M1 {m1}")
    except Exception:
        pass

    # CPI
    try:
        time.sleep(0.5)
        df_cpi = ak.macro_china_cpi()
        if df_cpi is not None and len(df_cpi) > 0:
            latest = df_cpi.iloc[0]
            policies["CPI"] = {
                "数值": str(latest.get("数值", "N/A"))[:20],
                "时间": str(latest.get("月份", ""))[:10]
            }
            print(f"  ✅ CPI: {policies['CPI']['时间']} {policies['CPI']['数值']}")
    except Exception:
        pass

    # 公开市场操作
    try:
        time.sleep(0.5)
        df_open = ak.macro_china_open_market_operation()
        if df_open is not None and len(df_open) > 0:
            latest = df_open.iloc[0]
            policies["公开市场"] = {
                "操作": str(latest.get("操作", ""))[:30],
                "规模": str(latest.get("交易量", latest.get("金额", "")))[:30],
                "中标利率": str(latest.get("中标利率", ""))[:10]
            }
            print(f"  ✅ 公开市场: {policies['公开市场']['操作']}")
    except Exception:
        pass

    # 财经新闻（关键词筛选）
    try:
        time.sleep(0.5)
        df_news = ak.stock_news_em()
        if df_news is not None and len(df_news) > 0:
            kw = ["央行", "美联储", "关税", "发改委", "财政部", "期货", "商品", "出口", "进口",
                  "产能", "环保", "供给侧", "降准", "加息", "大宗商品", "库存", "OPEC"]
            policy_news = []
            for _, row in df_news.iterrows():
                title = str(row.get("新闻标题", ""))
                if any(k in title for k in kw):
                    policy_news.append({
                        "title": title[:80],
                        "time": str(row.get("发布时间", ""))[:16],
                        "source": str(row.get("媒体名称", ""))[:20]
                    })
                if len(policy_news) >= 8:
                    break
            if policy_news:
                policies["政策资讯"] = policy_news
                print(f"  ✅ 政策相关资讯: {len(policy_news)} 条")
    except Exception:
        pass

    if not policies:
        print(f"  ⚠️ 政策数据采集失败，跳过")

    return policies


# ============================================================
#  Part 5：市场情绪
# ============================================================

def get_market_sentiment() -> dict:
    """获取市场情绪指标"""
    sentiment = {}
    print(f"\n😊 采集市场情绪...")

    # VIX
    try:
        time.sleep(0.5)
        df_vix = ak.index_vix()
        if df_vix is not None and len(df_vix) > 0:
            latest = df_vix.iloc[-1]
            vix = float(latest.get("VIX", latest.get("vix", 20)))
            sentiment["VIX"] = {
                "value": round(vix, 2),
                "level": "高波动" if vix > 25 else "正常" if vix > 15 else "低波动"
            }
            print(f"  ✅ VIX: {vix:.2f} ({sentiment['VIX']['level']})")
    except Exception:
        pass

    # 南华商品指数
    try:
        time.sleep(0.5)
        df_nh = ak.futures_index_ccidx(symbol="南华商品指数")
        if df_nh is not None and len(df_nh) > 0:
            latest = df_nh.iloc[-1]
            prev = df_nh.iloc[-2] if len(df_nh) > 1 else latest
            nh_price = float(latest.get("收盘", latest.get("收盘价", 0)))
            nh_prev = float(prev.get("收盘", prev.get("收盘价", nh_price)))
            nh_chg = ((nh_price - nh_prev) / nh_prev * 100) if nh_prev != 0 else 0
            sentiment["南华商品指数"] = {
                "price": nh_price, "change_pct": round(nh_chg, 2)
            }
            print(f"  ✅ 南华商品指数: {nh_price} ({nh_chg:+.2f}%)")
    except Exception:
        pass

    # 美元指数
    try:
        time.sleep(0.5)
        df_dxy = ak.currency_usdx()
        if df_dxy is not None and len(df_dxy) > 0:
            latest = df_dxy.iloc[-1]
            dxy = float(latest.get("指数", latest.get("收盘", 0)))
            sentiment["美元指数"] = {"value": dxy}
            print(f"  ✅ 美元指数: {dxy:.2f}")
    except Exception:
        pass

    if not sentiment:
        print(f"  ⚠️ 市场情绪数据暂不可用，跳过")

    return sentiment


# ============================================================
#  Part 6：财经新闻
# ============================================================

def get_futures_news() -> list:
    """获取财经新闻"""
    news_list = []

    try:
        time.sleep(0.5)
        df = ak.stock_news_em()
        if df is not None and len(df) > 0:
            for _, row in df.head(20).iterrows():
                news_list.append({
                    "title": str(row.get("新闻标题", row.get("title", "")))[:100],
                    "time": str(row.get("发布时间", ""))[:16],
                    "source": str(row.get("媒体名称", "东方财富"))[:20]
                })
    except Exception:
        pass

    # 去重
    seen = set()
    unique = []
    for n in news_list:
        t = n["title"].strip()
        if t and t not in seen:
            seen.add(t)
            unique.append(n)

    return unique[:15]


# ============================================================
#  Part 7：主采集函数 v5.0
# ============================================================

def collect_all_data(categories: dict) -> dict:
    """一站式采集全部数据 v5.0（含期现价差+机构持仓+信号评分）"""
    from config import ENABLE_LLM

    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "categories_data": {},
        "inventory_data": {},
        "inventory_trend": {},    # 库存环比趋势
        "economic_calendar": {},  # 经济数据日历
        "risk_data": {},          # 地缘政治风险
        "policy_data": {},
        "sentiment_data": {},
        "news": [],
        # v5.0 新增
        "basis_data": {},         # 期现价差分析
        "positions_data": {},     # 机构持仓排名
        "scores_data": {},        # 信号强度评分
    }

    # ── Step 1: 行情 ──────────────────────────────────────────
    print("\n" + "=" * 55)
    print("📡 Step 1: 采集行情数据...")
    result["categories_data"] = get_all_categories_data(categories)
    valid = sum(1 for v in result["categories_data"].values() if v.get("status") == "OK")
    print(f"  → 有效数据: {valid}/{len(categories)} 品种")

    if valid == 0:
        raise Exception("行情数据采集失败，请检查网络连接")

    # ── Step 2: 技术指标 ──────────────────────────────────────
    if ENABLE_LLM:
        print("\n" + "=" * 55)
        print("📈 Step 2: 计算技术指标...")
        result["categories_data"] = enrich_with_indicators(result["categories_data"])

    # ── Step 3: 库存原始数据 ──────────────────────────────────
    if ENABLE_LLM:
        print("\n" + "=" * 55)
        print("📦 Step 3: 采集库存数据...")
        result["inventory_data"] = get_inventory_data()

    # ── Step 3b: 库存环比趋势分析 ─────────────────────────────
    if ENABLE_LLM:
        print("\n" + "=" * 55)
        print("📊 Step 3b: 库存环比趋势分析...")
        result["inventory_trend"] = get_inventory_trend_analysis()

    # ── Step 3c: 经济数据日历 ─────────────────────────────────
    if ENABLE_LLM:
        print("\n" + "=" * 55)
        print("📅 Step 3c: 获取下周经济数据日历...")
        result["economic_calendar"] = get_economic_calendar()

    # ── Step 4: 政策宏观 ──────────────────────────────────────
    if ENABLE_LLM:
        print("\n" + "=" * 55)
        print("📋 Step 4: 采集政策与宏观数据...")
        result["policy_data"] = get_policy_data()

    # ── Step 5: 市场情绪 ──────────────────────────────────────
    if ENABLE_LLM:
        print("\n" + "=" * 55)
        print("😊 Step 5: 采集市场情绪...")
        result["sentiment_data"] = get_market_sentiment()

    # ── Step 6: 新闻 ──────────────────────────────────────────
    print("\n" + "=" * 55)
    print("📰 Step 6: 采集财经新闻...")
    result["news"] = get_futures_news()
    print(f"  → 获取 {len(result['news'])} 条")

    # ── Step 6b: 地缘政治风险识别 ────────────────────────────
    if ENABLE_LLM:
        print("\n" + "=" * 55)
        print("🚨 Step 6b: 地缘政治风险识别...")
        result["risk_data"] = detect_risks(result["news"])
        result["categories_data"] = tag_product_risks(result["risk_data"], result["categories_data"])

    # ── Step 6c: 库存-价格背离分析 ───────────────────────────
    if ENABLE_LLM and result.get("inventory_trend"):
        print("\n" + "=" * 55)
        print("⚠ Step 6c: 库存-价格背离分析...")
        result["inventory_trend"] = enrich_with_price_divergence(
            result["inventory_trend"], result["categories_data"]
        )

    # ── Step 7: 期现价差分析（v5.0 新增） ────────────────────
    if ENABLE_LLM:
        print("\n" + "=" * 55)
        print("💰 Step 7: 期现价差分析（v5.0新增）...")
        try:
            result["basis_data"] = analyze_basis(result["categories_data"])
            arb_count = len(result["basis_data"].get("arb_opportunities", []))
            print(f"  → 发现 {arb_count} 个套利机会")
        except Exception as e:
            print(f"  ⚠️ 期现价差分析失败: {e}")
            result["basis_data"] = {"status": "error", "error": str(e)}

    # ── Step 8: 机构持仓排名（v5.0 新增） ────────────────────
    if ENABLE_LLM:
        print("\n" + "=" * 55)
        print("🏦 Step 8: 机构持仓排名（v5.0新增）...")
        try:
            result["positions_data"] = get_positions_summary(result["categories_data"])
            sentiment_str = result["positions_data"].get("money_flow", {}).get("overall_sentiment", "-")
            print(f"  → 整体情绪: {sentiment_str}")
        except Exception as e:
            print(f"  ⚠️ 机构持仓分析失败: {e}")
            result["positions_data"] = {"status": "error", "error": str(e)}

    # ── Step 9: 信号强度评分（v5.0 新增） ────────────────────
    if ENABLE_LLM:
        print("\n" + "=" * 55)
        print("🎯 Step 9: 信号强度评分（v5.0新增）...")
        try:
            result["scores_data"] = score_all_symbols(
                categories_data=result["categories_data"],
                inventory_trend=result["inventory_trend"],
                basis_data=result["basis_data"],
                positions_data=result["positions_data"],
                policy_data=result["policy_data"],
                sentiment_data=result["sentiment_data"],
                risk_data=result["risk_data"],
                news=result["news"],
            )
            strong_buy = result["scores_data"].get("summary", {}).get("strong_buy", [])
            print(f"  → 强烈推荐品种: {strong_buy[:3]}")
        except Exception as e:
            print(f"  ⚠️ 信号评分失败: {e}")
            result["scores_data"] = {"status": "error", "error": str(e)}

    # ── 汇总 ──────────────────────────────────────────────────
    print("\n" + "=" * 55)
    print("✅ 全部数据采集完成！")
    print(f"  行情: {valid} 品种")
    print(f"  库存: {len(result['inventory_data'])} 品种")
    print(f"  期现价差: {len(result['basis_data'].get('basis_list', []))} 品种")
    print(f"  机构持仓: {result['positions_data'].get('money_flow', {}).get('total_analyzed', 0)} 品种")
    print(f"  信号评分: {result['scores_data'].get('summary', {}).get('total', 0)} 品种")

    return result
