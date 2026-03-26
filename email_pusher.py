# ==================== 邮件推送模块 ====================
"""
通过 QQ 邮箱 SMTP 发送分析报告
"""

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from datetime import datetime


def send_email(
    subject: str,
    html_body: str,
    text_body: str,
    config: dict
) -> bool:
    """
    通过 QQ 邮箱 SMTP 发送邮件
    
    Args:
        subject: 邮件主题
        html_body: HTML格式正文（用于展示）
        text_body: 纯文本正文（备用）
        config: {
            "EMAIL_HOST": "smtp.qq.com",
            "EMAIL_PORT": 465,
            "EMAIL_USE_SSL": True,
            "EMAIL_USERNAME": "xxx@qq.com",
            "EMAIL_PASSWORD": "授权码",
            "EMAIL_TO": "收件人"
        }
    """
    host = config.get("EMAIL_HOST", "smtp.qq.com")
    port = config.get("EMAIL_PORT", 465)
    use_ssl = config.get("EMAIL_USE_SSL", True)
    username = config.get("EMAIL_USERNAME", "")
    password = config.get("EMAIL_PASSWORD", "")
    to_addr = config.get("EMAIL_TO", username)

    if not username or not password or password == "你的授权码":
        print("⚠️ 邮箱未配置，跳过推送")
        return False

    try:
        # 构建邮件
        msg = MIMEMultipart("alternative")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"] = username  # QQ邮箱直接用账号
        msg["To"] = to_addr
        msg["Reply-To"] = username

        # 纯文本版本
        msg.attach(MIMEText(text_body, "plain", "utf-8"))
        # HTML版本
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        # 发送
        if use_ssl:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, context=context, timeout=20) as server:
                server.login(username, password)
                server.sendmail(username, [to_addr], msg.as_string())
        else:
            with smtplib.SMTP(host, port, timeout=20) as server:
                server.login(username, password)
                server.sendmail(username, [to_addr], msg.as_string())

        print(f"✅ 邮件发送成功 → {to_addr}")
        return True

    except smtplib.SMTPAuthenticationError:
        print("❌ 邮件认证失败，请检查邮箱账号和授权码是否正确")
        return False
    except smtplib.SMTPConnectError:
        print("❌ 邮件服务器连接失败，请检查网络")
        return False
    except smtplib.SMTPException as e:
        print(f"❌ 邮件发送失败: {e}")
        return False
    except Exception as e:
        print(f"❌ 邮件异常: {e}")
        return False


def format_email_html(category_data: dict, analysis: str, news: list) -> tuple:
    """
    格式化邮件内容，返回 (subject, html_body, text_body)
    """
    now = datetime.now()
    date_str = now.strftime("%Y年%m月%d日 %H:%M")

    valid_data = [v for v in category_data.values() if v.get("status") == "OK"]
    rising = [v for v in valid_data if v.get("change_pct", 0) > 0]
    falling = [v for v in valid_data if v.get("change_pct", 0) < 0]
    avg_change = sum(v.get("change_pct", 0) for v in valid_data) / len(valid_data) if valid_data else 0

    # 涨幅/跌幅前3
    sorted_all = sorted(valid_data, key=lambda x: x.get("change_pct", 0), reverse=True)
    top3 = sorted_all[:3]
    bot3 = sorted_all[-3:]

    # === Subject ===
    subject = f"📊 期货早参 {now.strftime('%m月%d日 %H:%M')} | 全品种AI分析"

    # === HTML Body ===
    html_parts = [
        f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body {{ font-family: -apple-system, 'PingFang SC', 'Microsoft YaHei', sans-serif; max-width: 700px; margin: 0 auto; padding: 20px; background: #f5f6fa; }}
  .card {{ background: white; border-radius: 12px; padding: 20px; margin-bottom: 16px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
  .header {{ text-align: center; padding: 24px 20px; background: linear-gradient(135deg, #1a1a2e, #16213e); border-radius: 12px; color: white; margin-bottom: 20px; }}
  .header h1 {{ margin: 0; font-size: 22px; }}
  .header p {{ margin: 8px 0 0; color: #aaa; font-size: 13px; }}
  .stats {{ display: flex; gap: 12px; margin-top: 16px; }}
  .stat {{ flex: 1; background: rgba(255,255,255,0.1); border-radius: 8px; padding: 12px; text-align: center; }}
  .stat .num {{ font-size: 22px; font-weight: bold; }}
  .stat .label {{ font-size: 12px; color: #ccc; margin-top: 4px; }}
  .rise {{ color: #e74c3c; }}
  .fall {{ color: #27ae60; }}
  h2 {{ color: #1a1a2e; font-size: 15px; margin: 0 0 12px; border-left: 4px solid #3498db; padding-left: 10px; }}
  .table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  .table th {{ background: #f8f9fa; padding: 8px 10px; text-align: left; color: #666; font-weight: normal; }}
  .table td {{ padding: 8px 10px; border-top: 1px solid #eee; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; }}
  .badge-long {{ background: #ffeaea; color: #e74c3c; }}
  .badge-short {{ background: #eafff0; color: #27ae60; }}
  .badge-neutral {{ background: #f0f0f0; color: #888; }}
  .news {{ font-size: 13px; line-height: 1.8; color: #555; }}
  .news li {{ margin-bottom: 6px; }}
  .analysis {{ background: #f8f9fa; border-radius: 8px; padding: 16px; font-size: 13px; line-height: 1.8; white-space: pre-wrap; }}
  .footer {{ text-align: center; color: #aaa; font-size: 11px; margin-top: 20px; }}
  .warning {{ background: #fff3cd; border: 1px solid #ffc107; border-radius: 8px; padding: 12px; font-size: 12px; color: #856404; margin-top: 12px; }}
</style>
</head>
<body>

<div class="header">
  <h1>📊 国内期货智能早参</h1>
  <p>{date_str} · 全自动AI分析</p>
  <div class="stats">
    <div class="stat">
      <div class="num">{len(valid_data)}</div>
      <div class="label">监测品种</div>
    </div>
    <div class="stat">
      <div class="num rise">{len(rising)}</div>
      <div class="label">上涨 🔺</div>
    </div>
    <div class="stat">
      <div class="num fall">{len(falling)}</div>
      <div class="label">下跌 🔻</div>
    </div>
    <div class="stat">
      <div class="num {'rise' if avg_change > 0 else 'fall'}">{avg_change:+.2f}%</div>
      <div class="label">市场平均</div>
    </div>
  </div>
</div>
"""
    ]

    # === 涨幅前3 ===
    html_parts.append("""
<div class="card">
  <h2>🔥 涨幅榜 TOP3</h2>
  <table class="table">
    <tr><th>品种</th><th>最新价</th><th>涨跌幅</th><th>趋势</th></tr>""")
    for item in top3:
        trend = item.get("trend", "震荡")
        badge = {"多头": "badge-long", "空头": "badge-short"}.get(trend, "badge-neutral")
        trend_icon = "📈" if trend == "多头" else ("📉" if trend == "空头" else "➡️")
        html_parts.append(
            f'<tr><td><b>{item["product"]}</b></td>'
            f'<td>{item["price"]}</td>'
            f'<td class="rise"><b>{item["change_pct"]:+.2f}%</b></td>'
            f'<td><span class="badge {badge}">{trend_icon}{trend}</span></td></tr>'
        )
    html_parts.append("</table></div>")

    # === 跌幅前3 ===
    html_parts.append("""
<div class="card">
  <h2>❄️ 跌幅榜 TOP3</h2>
  <table class="table">
    <tr><th>品种</th><th>最新价</th><th>涨跌幅</th><th>趋势</th></tr>""")
    for item in bot3:
        trend = item.get("trend", "震荡")
        badge = {"多头": "badge-long", "空头": "badge-short"}.get(trend, "badge-neutral")
        trend_icon = "📈" if trend == "多头" else ("📉" if trend == "空头" else "➡️")
        html_parts.append(
            f'<tr><td><b>{item["product"]}</b></td>'
            f'<td>{item["price"]}</td>'
            f'<td class="fall"><b>{item["change_pct"]:+.2f}%</b></td>'
            f'<td><span class="badge {badge}">{trend_icon}{trend}</span></td></tr>'
        )
    html_parts.append("</table></div>")

    # === AI分析 ===
    if analysis and not analysis.startswith("["):
        html_parts.append(f"""
<div class="card">
  <h2>🧠 AI 智能分析</h2>
  <div class="analysis">{analysis}</div>
  <div class="warning">⚠️ 本分析仅供参考，不构成投资建议。期市有风险，入市需谨慎。</div>
</div>""")

    # === 新闻 ===
    if news:
        html_parts.append("""
<div class="card">
  <h2>📰 最新资讯</h2>
  <ul class="news">""")
        for n in news[:6]:
            html_parts.append(f'<li>• {n["title"]} <span style="color:#999">({n["source"]})</span></li>')
        html_parts.append("</ul></div>")

    # === 页脚 ===
    html_parts.append(f"""
<div class="footer">
  <p>由 AI 期货分析系统自动生成 · {date_str}</p>
  <p>⚠️ 仅供参考，不构成投资建议</p>
</div>

</body>
</html>""")

    html_body = "\n".join(html_parts)

    # === Text Body ===
    sorted_all = sorted(valid_data, key=lambda x: x.get("change_pct", 0), reverse=True)
    text_lines = [
        f"📊 国内期货早参 {date_str}",
        "=" * 40,
        f"监测品种: {len(valid_data)} 个  上涨: {len(rising)}  下跌: {len(falling)}  平均: {avg_change:+.2f}%",
        "",
        "【涨幅榜 TOP3】",
    ]
    for item in top3:
        text_lines.append(f"  {item['product']} {item['price']} ({item['change_pct']:+.2f}%) {item.get('trend','')}")
    text_lines.append("\n【跌幅榜 TOP3】")
    for item in bot3:
        text_lines.append(f"  {item['product']} {item['price']} ({item['change_pct']:+.2f}%) {item.get('trend','')}")
    if analysis and not analysis.startswith("["):
        text_lines.append(f"\n【AI分析】\n{analysis}")
    if news:
        text_lines.append(f"\n【资讯】")
        for n in news[:5]:
            text_lines.append(f"  • {n['title']}")
    text_lines.append("\n" + "=" * 40)
    text_lines.append("⚠️ 仅供参考，不构成投资建议")
    text_body = "\n".join(text_lines)

    return subject, html_body, text_body


def format_email_html_v2(all_data: dict, analysis: str) -> tuple:
    """
    v2版全维度邮件格式
    包含：行情总览 + 技术面 + 库存 + 政策 + 市场情绪 + AI分析
    """
    now = datetime.now()
    date_str = now.strftime("%Y年%m月%d日 %H:%M")

    cat_data = all_data.get("categories_data", {})
    inventory = all_data.get("inventory_data", {})
    policy = all_data.get("policy_data", {})
    sentiment = all_data.get("sentiment_data", {})
    news = all_data.get("news", [])

    valid = [v for v in cat_data.values() if v.get("status") == "OK"]
    rising = [v for v in valid if v.get("change_pct", 0) > 0]
    falling = [v for v in valid if v.get("change_pct", 0) < 0]
    avg = sum(v.get("change_pct", 0) for v in valid) / len(valid) if valid else 0

    sorted_all = sorted(valid, key=lambda x: x.get("change_pct", 0), reverse=True)
    top5 = sorted_all[:5]
    bot5 = sorted_all[-5:]

    # 板块统计
    cats_stats = {}
    for item in valid:
        cat = item.get("category", "其他")
        if cat not in cats_stats:
            cats_stats[cat] = []
        cats_stats[cat].append(item.get("change_pct", 0))

    subject = f"📊 期货早参 {now.strftime('%m月%d日 %H:%M')} | 全维度AI分析"

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<style>
  body{{font-family:-apple-system,'PingFang SC','Microsoft YaHei',sans-serif;max-width:720px;margin:0 auto;padding:16px;background:#f0f2f5}}
  .header{{background:linear-gradient(135deg,#1a1a2e,#16213e);border-radius:16px;padding:24px 20px;color:white;margin-bottom:20px;text-align:center}}
  .header h1{{margin:0;font-size:20px}}
  .header p{{margin:8px 0 0;color:#9ab;color;font-size:12px}}
  .stats{{display:flex;gap:8px;margin-top:16px;flex-wrap:wrap;justify-content:center}}
  .stat{{flex:1;min-width:70px;background:rgba(255,255,255,0.1);border-radius:10px;padding:10px 6px;text-align:center}}
  .stat .n{{font-size:20px;font-weight:bold}}
  .stat .l{{font-size:11px;color:#ccc;margin-top:2px}}
  .card{{background:white;border-radius:12px;padding:16px;margin-bottom:14px;box-shadow:0 2px 8px rgba(0,0,0,0.06)}}
  h2{{color:#1a1a2e;font-size:14px;margin:0 0 12px;border-left:4px solid #3498db;padding-left:10px}}
  h2.orange{{border-color:#e67e22}}
  h2.red{{border-color:#e74c3c}}
  h2.green{{border-color:#27ae60}}
  h2.purple{{border-color:#9b59b6}}
  table{{width:100%;border-collapse:collapse;font-size:12px}}
  th{{background:#f8f9fb;padding:7px 8px;text-align:left;color:#666;font-weight:normal}}
  td{{padding:7px 8px;border-top:1px solid #eee}}
  .rise{{color:#e74c3c;font-weight:bold}}
  .fall{{color:#27ae60;font-weight:bold}}
  .badge{{display:inline-block;padding:1px 7px;border-radius:8px;font-size:10px}}
  .b-long{{background:#ffeaea;color:#c0392b}}
  .b-short{{background:#eafff0;color:#27ae60}}
  .b-neutral{{background:#f0f0f0;color:#888}}
  .tag{{display:inline-block;background:#3498db;color:white;border-radius:4px;padding:1px 5px;font-size:10px;margin-left:4px}}
  .ai-box{{background:#f8f9fb;border-radius:10px;padding:14px;font-size:12px;line-height:1.9;white-space:pre-wrap;max-height:400px;overflow-y:auto}}
  .warning{{background:#fff3cd;border:1px solid #ffc107;border-radius:8px;padding:10px;font-size:12px;color:#856404;margin-top:12px}}
  .news{{font-size:12px;line-height:1.8}}
  .news li{{margin-bottom:5px;color:#555}}
  .footer{{text-align:center;color:#aaa;font-size:11px;margin-top:20px}}
  .dim{{background:#f0f2f5;border-radius:8px;padding:10px 14px;font-size:12px;margin-bottom:10px}}
  .dim-name{{font-weight:bold;color:#333}}
  .dim-val{{color:#555;margin-top:3px}}
  .section-gap{{margin-top:14px}}
</style>
</head><body>

<div class="header">
  <h1>📊 国内期货 · 全维度智能早参</h1>
  <p>{date_str} · 政策 · 库存 · 情绪 · 技术面 四维分析</p>
  <div class="stats">
    <div class="stat"><div class="n">{len(valid)}</div><div class="l">监测品种</div></div>
    <div class="stat"><div class="n rise">{len(rising)}</div><div class="l">上涨 🔺</div></div>
    <div class="stat"><div class="n fall">{len(falling)}</div><div class="l">下跌 🔻</div></div>
    <div class="stat"><div class="n {'rise' if avg>0 else 'fall'}">{avg:+.2f}%</div><div class="l">市场平均</div></div>
  </div>
</div>
"""

    # === 板块动向 ===
    html += '<div class="card"><h2>🔥 板块动向</h2><div style="display:flex;flex-wrap:wrap;gap:8px">'
    for cat, changes in sorted(cats_stats.items(), key=lambda x: sum(x[1])/len(x[1]) if x[1] else 0, reverse=True):
        avg_c = sum(changes) / len(changes)
        color = "#e74c3c" if avg_c > 1 else "#27ae60" if avg_c < -0.5 else "#888"
        html += f'<div style="flex:1;min-width:90px;background:#f8f9fb;border-radius:8px;padding:8px;text-align:center">'
        html += f'<div style="font-size:13px;font-weight:bold;color:{color}">{avg_c:+.2f}%</div>'
        html += f'<div style="font-size:11px;color:#888">{cat}</div></div>'
    html += '</div></div>'

    # === 技术面重点品种 ===
    focus = top5
    html += f'<div class="card section-gap"><h2 class="orange">📈 技术面重点品种（涨幅TOP5）</h2><table>'
    html += '<tr><th>品种</th><th>价格</th><th>涨跌幅</th><th>趋势</th><th>RSI</th><th>MACD</th><th>布林带</th><th>信号</th></tr>'
    for item in focus:
        p = item.get("product","")
        price = item.get("price", 0)
        chg = item.get("change_pct", 0)
        trend = item.get("trend","震荡")
        tech = item.get("tech", {})
        rsi = tech.get("rsi","-")
        macd_hist = tech.get("macd_hist", 0)
        bb_pos = tech.get("bb_position", 0.5)
        gc = tech.get("golden_cross", False)
        dc = tech.get("dead_cross", False)
        badge = {"多头":"b-long","空头":"b-short"}.get(trend,"b-neutral")
        t_icon = "📈" if trend=="多头" else "📉" if trend=="空头" else "➡️"
        macd_icon = "▲" if macd_hist>0 else "▼"
        bb_pos_icon = "上" if bb_pos>0.8 else "下" if bb_pos<0.2 else "中"
        signal = ""
        if gc: signal="<span class='tag' style='background:#e74c3c'>金叉</span>"
        elif dc: signal="<span class='tag' style='background:#27ae60'>死叉</span>"
        html += (f'<tr>'
                 f'<td><b>{p}</b></td>'
                 f'<td>{price}</td>'
                 f'<td class="rise">{chg:+.2f}%</td>'
                 f'<td><span class="badge {badge}">{t_icon}{trend}</span></td>'
                 f'<td>{rsi}</td>'
                 f'<td>{macd_icon}{macd_hist:+.2f}</td>'
                 f'<td>{bb_pos_icon}({bb_pos:.0%})</td>'
                 f'<td>{signal}</td>'
                 f'</tr>')
    html += '</table></div>'

    # === 跌幅品种 ===
    if bot5:
        html += f'<div class="card"><h2 class="green">📉 跌幅前5</h2><table>'
        html += '<tr><th>品种</th><th>价格</th><th>涨跌幅</th><th>趋势</th><th>RSI</th><th>MACD</th><th>布林带</th><th>信号</th></tr>'
        for item in bot5:
            p = item.get("product","")
            price = item.get("price", 0)
            chg = item.get("change_pct", 0)
            trend = item.get("trend","震荡")
            tech = item.get("tech", {})
            rsi = tech.get("rsi","-")
            macd_hist = tech.get("macd_hist", 0)
            bb_pos = tech.get("bb_position", 0.5)
            gc = tech.get("golden_cross", False)
            dc = tech.get("dead_cross", False)
            badge = {"多头":"b-long","空头":"b-short"}.get(trend,"b-neutral")
            t_icon = "📈" if trend=="多头" else "📉" if trend=="空头" else "➡️"
            macd_icon = "▲" if macd_hist>0 else "▼"
            bb_pos_icon = "上" if bb_pos>0.8 else "下" if bb_pos<0.2 else "中"
            signal = ""
            if gc: signal="<span class='tag' style='background:#e74c3c'>金叉</span>"
            elif dc: signal="<span class='tag' style='background:#27ae60'>死叉</span>"
            html += (f'<tr>'
                     f'<td><b>{p}</b></td>'
                     f'<td>{price}</td>'
                     f'<td class="fall">{chg:+.2f}%</td>'
                     f'<td><span class="badge {badge}">{t_icon}{trend}</span></td>'
                     f'<td>{rsi}</td>'
                     f'<td>{macd_icon}{macd_hist:+.2f}</td>'
                     f'<td>{bb_pos_icon}({bb_pos:.0%})</td>'
                     f'<td>{signal}</td>'
                     f'</tr>')
        html += '</table></div>'

    # === 库存数据 ===
    if inventory:
        html += f'<div class="card section-gap"><h2 class="purple">📦 库存数据</h2>'
        for name, inv in list(inventory.items())[:6]:
            html += f'<div class="dim"><span class="dim-name">【{name}】</span>'
            html += f'<div class="dim-val">{inv.get("库存","N/A")} {inv.get("变化","")} {inv.get("单位","")}</div></div>'
        html += '</div>'

    # === 政策与宏观 ===
    has_policy = policy.get("货币供应") or policy.get("CPI") or policy.get("公开市场") or policy.get("政策资讯")
    if has_policy:
        html += f'<div class="card"><h2>📋 政策与宏观</h2>'
        if policy.get("货币供应"):
            m = policy["货币供应"]
            html += f'<div class="dim"><span class="dim-name">货币供应</span><div class="dim-val">M2同比 {m.get("M2同比","N/A")} | M1同比 {m.get("M1同比","N/A")}</div></div>'
        if policy.get("CPI"):
            c = policy["CPI"]
            html += f'<div class="dim"><span class="dim-name">CPI数据</span><div class="dim-val">{c.get("时间","")} {c.get("数值","N/A")}</div></div>'
        if policy.get("公开市场"):
            o = policy["公开市场"]
            html += f'<div class="dim"><span class="dim-name">公开市场</span><div class="dim-val">{o.get("操作","")} {o.get("规模","")} 利率{o.get("中标利率","")}</div></div>'
        if policy.get("政策资讯"):
            html += '<div style="font-size:12px;margin-top:8px"><b>相关政策：</b>'
            for n in policy["政策资讯"][:4]:
                html += f'<div style="margin-top:3px">• {n["title"]}</div>'
            html += '</div>'
        html += '</div>'

    # === 市场情绪 ===
    if sentiment:
        html += f'<div class="card"><h2>😊 市场情绪</h2><div style="display:flex;flex-wrap:wrap;gap:8px">'
        if sentiment.get("VIX"):
            v = sentiment["VIX"]
            color = "#e74c3c" if v["value"]>25 else "#27ae60"
            html += f'<div style="flex:1;min-width:100px;background:#f8f9fb;border-radius:8px;padding:10px;text-align:center">'
            html += f'<div style="font-size:16px;font-weight:bold;color:{color}">{v["value"]}</div>'
            html += f'<div style="font-size:11px;color:#888">VIX恐慌指数</div>'
            html += f'<div style="font-size:11px;color:{color}">{v["level"]}</div></div>'
        if sentiment.get("南华商品指数"):
            s = sentiment["南华商品指数"]
            c2 = "#e74c3c" if s["change_pct"]>0 else "#27ae60"
            html += f'<div style="flex:1;min-width:100px;background:#f8f9fb;border-radius:8px;padding:10px;text-align:center">'
            html += f'<div style="font-size:16px;font-weight:bold">{s["price"]}</div>'
            html += f'<div style="font-size:11px;color:{c2}">{s["change_pct"]:+.2f}%</div>'
            html += f'<div style="font-size:11px;color:#888">南华商品指数</div></div>'
        if sentiment.get("美元指数"):
            d = sentiment["美元指数"]
            html += f'<div style="flex:1;min-width:100px;background:#f8f9fb;border-radius:8px;padding:10px;text-align:center">'
            html += f'<div style="font-size:16px;font-weight:bold">{d["value"]:.2f}</div>'
            html += f'<div style="font-size:11px;color:#888">美元指数</div></div>'
        html += '</div></div>'

    # === AI分析 ===
    if analysis and not analysis.startswith("["):
        html += f'<div class="card section-gap"><h2>🧠 AI 全维度综合分析</h2>'
        html += f'<div class="ai-box">{analysis}</div>'
        html += '<div class="warning">⚠️ 本分析仅供参考，不构成投资建议。期市有风险，入市需谨慎。</div></div>'

    # === 新闻 ===
    if news:
        html += f'<div class="card"><h2>📰 最新资讯</h2><ul class="news">'
        for n in news[:6]:
            html += f'<li>{n["title"]} <span style="color:#999">({n["source"]} {n["time"]})</span></li>'
        html += '</ul></div>'

    html += f'<div class="footer"><p>🤖 由 AI 期货全维度分析系统生成 · {date_str}</p><p>⚠️ 仅供参考，不构成投资建议</p></div></body></html>'

    # === Text版本 ===
    text_lines = [
        f"📊 期货全维度早参 {date_str}",
        "=" * 40,
        f"品种: {len(valid)}个  涨: {len(rising)}  跌: {len(falling)}  均: {avg:+.2f}%",
    ]
    for item in top5:
        text_lines.append(f"🔺 {item['product']} {item['price']} ({item['change_pct']:+.2f}%) {item.get('trend','')}")
    if analysis and not analysis.startswith("["):
        text_lines.append(f"\n【AI分析】\n{analysis}")
    text_lines.append("\n" + "=" * 40)
    text_lines.append("⚠️ 仅供参考，不构成投资建议")
    text_body = "\n".join(text_lines)

    return subject, html, text_body


if __name__ == "__main__":
    # 测试发送
    from config import EMAIL_HOST, EMAIL_PORT, EMAIL_USE_SSL, EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_TO
    cfg = {
        "EMAIL_HOST": EMAIL_HOST,
        "EMAIL_PORT": EMAIL_PORT,
        "EMAIL_USE_SSL": EMAIL_USE_SSL,
        "EMAIL_USERNAME": EMAIL_USERNAME,
        "EMAIL_PASSWORD": EMAIL_PASSWORD,
        "EMAIL_TO": EMAIL_TO,
    }
    ok = send_email(
        subject="🧪 期货分析系统 测试邮件",
        html_body="<p>这是一封<strong>测试邮件</strong>，系统运行正常！</p>",
        text_body="这是一封测试邮件，系统运行正常！",
        config=cfg
    )
    print("发送结果:", ok)
