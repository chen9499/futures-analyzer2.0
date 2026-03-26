# ==================== 微信推送模块 ====================
"""
通过 Server酱 将分析报告推送到微信
免费额度，无需服务器
注册地址: https://sct.ftqq.com/
"""

import requests
from datetime import datetime


def push_to_wechat(title: str, content: str, sendkey: str) -> bool:
    """
    通过 Server酱 推送消息到微信
    
    Args:
        title: 消息标题（微信会显示）
        content: 消息内容（支持markdown）
        sendkey: Server酱的SendKey
    
    Returns:
        bool: 推送是否成功
    """
    if sendkey == "你的Server酱SendKey" or not sendkey:
        print("⚠️ Server酱未配置，模拟推送内容：")
        print(f"  标题: {title}")
        print(f"  内容: {content[:200]}...")
        return False
    
    # Server酱新版本API（GET方式）
    # 新接口：https://sct.ftqq.com/send/{sendkey}?title=xxx&desp=xxx
    url = f"https://sct.ftqq.com/send/{sendkey}"
    
    # 构建简洁版内容（微信文本限制）
    short_content = content[:1900]  # 留余量
    
    params = {
        "title": title,
        "desp": short_content,
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        text = response.text.strip()
        
        # 尝试解析JSON
        try:
            result = response.json()
            if result.get("code") == 0 or result.get("errno") == 0:
                print(f"✅ 微信推送成功")
                return True
            elif result.get("code") == -1:
                print(f"❌ Server酱验证失败，请检查SendKey是否正确")
                return False
            elif result.get("code") == 40001:
                print(f"❌ Server酱：微信推送通道未激活")
                print(f"   💡 请访问 https://sct.ftqq.com/ 扫码绑定微信")
                return False
            else:
                print(f"⚠️ 推送异常: {result}")
                return False
        except ValueError:
            # 返回的不是JSON，可能是账号问题
            print(f"⚠️ Server酱返回非JSON响应")
            print(f"   响应前100字符: {text[:100]}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ 推送超时（15秒），请检查网络连接")
        return False
    except requests.exceptions.ConnectionError:
        print("❌ 网络连接失败，请检查网络")
        return False
    except Exception as e:
        print(f"❌ 推送失败: {e}")
        return False


def format_report_markdown(
    category_data: dict, 
    analysis_result: str, 
    news: list
) -> str:
    """
    将数据格式化为精美的markdown报告
    适配微信展示
    """
    
    now = datetime.now()
    date_str = now.strftime("%Y年%m月%d日")
    time_str = now.strftime("%H:%M")
    
    # === 统计数据 ===
    valid_data = [v for v in category_data.values() if v.get("status") == "OK"]
    if valid_data:
        rising = [v for v in valid_data if v.get("change_pct", 0) > 0]
        falling = [v for v in valid_data if v.get("change_pct", 0) < 0]
        avg_change = sum(v.get("change_pct", 0) for v in valid_data) / len(valid_data)
    else:
        rising, falling = [], []
        avg_change = 0
    
    # === 构建markdown ===
    lines = [
        f"# 📊 期货早参 | {date_str} {time_str}",
        "",
        "---",
        "",
        "## 📈 今日概览",
        "",
        f"| 统计项 | 数值 |",
        f"|--------|------|",
        f"| 监测品种 | {len(valid_data)} 个 |",
        f"| 上涨品种 | {len(rising)} 个 🔺 |",
        f"| 下跌品种 | {len(falling)} 个 🔻 |",
        f"| 市场平均 | {avg_change:+.2f}% |",
        "",
    ]
    
    # === LLM分析结果 ===
    if analysis_result and not analysis_result.startswith("["):
        lines.append("---")
        lines.append("")
        lines.append("## 🧠 AI 智能分析")
        lines.append("")
        # 只取AI分析的核心部分
        lines.append(analysis_result)
        lines.append("")
    else:
        # 无LLM时显示原始数据表
        lines.append("---")
        lines.append("")
        lines.append("## 📋 全品种一览")
        lines.append("")
        
        # 按涨跌幅排序
        sorted_data = sorted(valid_data, key=lambda x: x.get("change_pct", 0), reverse=True)
        
        lines.append("| 品种 | 最新价 | 涨跌幅 | 趋势 |")
        lines.append("|------|--------|--------|------|")
        for item in sorted_data:
            trend = item.get("trend", "震荡")
            trend_icon = "📈" if trend == "多头" else ("📉" if trend == "空头" else "➡️")
            change = item.get("change_pct", 0)
            price = item.get("price", 0)
            lines.append(f"| {item['product']:>4} | {price:>8} | {change:>+7.2f}% | {trend_icon}{trend} |")
        
        lines.append("")
    
    # === 新闻摘要 ===
    if news:
        lines.append("---")
        lines.append("")
        lines.append("## 📰 财经要闻")
        lines.append("")
        for n in news[:5]:
            lines.append(f"- {n['title']}")
        lines.append("")
        lines.append(f"> 共 {len(news)} 条资讯，点击查看全部")
    
    # === 底部 ===
    lines.append("---")
    lines.append("")
    lines.append("> 🤖 由 **期货智能分析系统** 自动生成")
    lines.append("> ⚠️ 仅供参考，不构成投资建议")
    
    return "\n".join(lines)


def format_short_report(category_data: dict) -> str:
    """
    简短版推送（用于微信纯文本）
    当markdown推送失败时降级使用
    """
    now = datetime.now()
    date_str = now.strftime("%m月%d日 %H:%M")
    
    valid_data = [v for v in category_data.values() if v.get("status") == "OK"]
    sorted_data = sorted(valid_data, key=lambda x: x.get("change_pct", 0), reverse=True)
    
    lines = [f"📊 期货早参 {date_str}\n"]
    lines.append(f"监测品种: {len(valid_data)} 个\n")
    
    if sorted_data:
        top = sorted_data[:3]
        bot = sorted_data[-3:]
        
        lines.append("\n🔥 涨幅前三:")
        for item in top:
            lines.append(f"  {item['product']} {item['price']} ({item['change_pct']:+.2f}%)")
        
        lines.append("\n❄️ 跌幅前三:")
        for item in bot:
            lines.append(f"  {item['product']} {item['price']} ({item['change_pct']:+.2f}%)")
    
    return "\n".join(lines)


if __name__ == "__main__":
    # 测试推送
    from config import SERVERCHAN_SENDKEY
    test_title = "🧪 期货分析系统 测试推送"
    test_content = "这是一条测试消息，系统运行正常！"
    push_to_wechat(test_title, test_content, SERVERCHAN_SENDKEY)
