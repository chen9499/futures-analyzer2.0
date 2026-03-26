# ==================== 主程序：每日定时运行 ====================
"""
期货智能分析系统 - 主程序
每天早上8:00自动运行：
1. 采集全品种行情数据
2. 计算技术指标
3. 获取最新新闻
4. Kimi大模型分析
5. 推送到微信
"""

import sys
import time
import traceback
from datetime import datetime

# 添加当前目录到路径
sys.path.insert(0, sys.path[0] if sys.path[0] else ".")

from config import (
    CATEGORIES, 
    PUSH_METHOD,
    EMAIL_HOST, EMAIL_PORT, EMAIL_USE_SSL, EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_TO,
    ENABLE_LLM, PUSH_HOUR, PUSH_MINUTE
)


def run_full_analysis() -> dict:
    """
    执行完整的期货分析流程
    返回: {"success": bool, "report": str, "data": dict}
    """
    start_time = time.time()
    report_data = {
        "success": False,
        "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": {},
        "news": [],
        "analysis": "",
        "report": "",
        "error": None
    }
    
    print("=" * 50)
    print("🚀 期货智能分析系统启动")
    print(f"⏰ 执行时间: {report_data['start_time']}")
    print("=" * 50)
    
    try:
        # ========== 第1步：采集全维度数据 ==========
        print("\n📡 采集全维度数据...")
        from data_collector import collect_all_data
        all_data = collect_all_data(CATEGORIES)
        category_data = all_data["categories_data"]
        inventory_data = all_data["inventory_data"]
        policy_data = all_data["policy_data"]
        sentiment_data = all_data["sentiment_data"]
        news = all_data["news"]
        
        report_data["data"] = category_data
        report_data["inventory"] = inventory_data
        report_data["policy"] = policy_data
        report_data["sentiment"] = sentiment_data
        
        valid_count = sum(1 for v in category_data.values() if v.get("status") == "OK")
        print(f"✅ 成功采集 {valid_count}/{len(category_data)} 个品种")
        
        if valid_count == 0:
            raise Exception("行情数据采集失败，所有品种均无数据")
        
        # ========== 第2步：LLM全维度分析 ==========
        if ENABLE_LLM:
            print("\n🧠 Kimi全维度分析...")
            from llm_analyzer import analyze_with_kimi_v2
            analysis = analyze_with_kimi_v2(all_data)
            report_data["analysis"] = analysis
            print(f"✅ 分析完成（{len(analysis)} 字）")
        else:
            print("\n⏭️ LLM分析已禁用")
            report_data["analysis"] = ""
        
        # ========== 第3步：生成报告 ==========
        print("\n📝 生成分析报告...")
        from wechat_pusher import format_report_markdown
        report = format_report_markdown(category_data, report_data["analysis"], news)
        report_data["report"] = report
        print(f"✅ 报告生成完成（{len(report)} 字符）")
        
        # ========== 第4步：推送 ==========
        print("\n📱 推送...")
        title = f"📊 期货早参 | {datetime.now().strftime('%m月%d日 %H:%M')}"
        
        if PUSH_METHOD == "email":
            email_cfg = {
                "EMAIL_HOST": EMAIL_HOST,
                "EMAIL_PORT": EMAIL_PORT,
                "EMAIL_USE_SSL": EMAIL_USE_SSL,
                "EMAIL_USERNAME": EMAIL_USERNAME,
                "EMAIL_PASSWORD": EMAIL_PASSWORD,
                "EMAIL_TO": EMAIL_TO,
            }
            # 使用v2全维度邮件格式
            from email_pusher import format_email_html_v2, send_email
            subject, html_body, text_body = format_email_html_v2(
                all_data, report_data["analysis"]
            )
            push_ok = send_email(subject, html_body, text_body, email_cfg)
        else:
            from wechat_pusher import push_to_wechat
            push_ok = push_to_wechat(title, report, SERVERCHAN_SENDKEY)

        report_data["push_ok"] = push_ok
        report_data["success"] = True

        # ========== 完成 ==========
        elapsed = time.time() - start_time
        inv_count = len(inventory_data)
        pol_count = len(policy_data.get("政策资讯", []))
        sent_count = len(sentiment_data)
        print("\n" + "=" * 50)
        print(f"✅ 全流程完成！耗时 {elapsed:.1f} 秒")
        print(f"📊 行情品种: {valid_count} 个")
        print(f"📈 技术指标: 已计算")
        print(f"📦 库存数据: {inv_count} 条")
        print(f"📋 政策宏观: {pol_count} 条")
        print(f"😊 市场情绪: {sent_count} 个指标")
        print(f"📰 财经新闻: {len(news)} 条")
        print(f"🤖 Kimi分析: {len(report_data['analysis'])} 字")
        print(f"📧 推送: {'✅ 成功' if push_ok else '❌ 失败'}")
        print("=" * 50)
        
        return report_data
        
    except Exception as e:
        error_msg = f"❌ 系统异常: {str(e)}"
        print(f"\n{error_msg}")
        print(traceback.format_exc())
        report_data["error"] = str(e)
        return report_data


def run_once():
    """立即执行一次分析（测试用）"""
    return run_full_analysis()


# ==================== 定时调度逻辑 ====================

def wait_until_target_time(hour: int, minute: int):
    """阻塞等待直到目标时间（仅用于standalone模式）"""
    from datetime import datetime, timedelta
    
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    
    if target <= now:
        target += timedelta(days=1)  # 明天
    
    wait_seconds = (target - now).total_seconds()
    print(f"⏰ 下次执行时间: {target.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏳ 距离下次执行还有 {int(wait_seconds//3600)} 小时 {int((wait_seconds%3600)//60)} 分钟")


def main():
    """主入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description="期货智能分析系统")
    parser.add_argument("--once", action="store_true", help="立即执行一次然后退出（不循环）")
    parser.add_argument("--config", action="store_true", help="显示当前配置")
    args = parser.parse_args()
    
    if args.config:
        print("\n📋 当前系统配置：")
        from config import (
            CATEGORIES, SERVERCHAN_SENDKEY, KIMI_API_KEY,
            ENABLE_LLM, PUSH_HOUR, PUSH_MINUTE, MODEL_NAME
        )
        print(f"  推送方式: Server酱 ({'已配置' if SERVERCHAN_SENDKEY != '你的Server酱SendKey' else '❌ 未配置'})")
        print(f"  LLM分析: {'✅ 开启 (Kimi ' + MODEL_NAME + ')' if ENABLE_LLM else '❌ 关闭'}")
        print(f"  Kimi Key: {'✅ 已配置' if KIMI_API_KEY != '你的Kimi API Key' else '❌ 未配置'}")
        print(f"  定时推送: 每天 {PUSH_HOUR:02d}:{PUSH_MINUTE:02d}")
        print(f"  监测品种: {len(CATEGORIES)} 个分类")
        for cat, symbols in CATEGORIES.items():
            print(f"    - {cat}: {', '.join(symbols)}")
        return
    
    if args.once:
        print("🔄 立即执行模式\n")
        run_full_analysis()
    else:
        # 定时循环模式
        print("⏰ 期货分析系统 - 定时运行模式")
        print(f"📅 每天 {PUSH_HOUR:02d}:{PUSH_MINUTE:02d} 自动执行\n")
        
        wait_until_target_time(PUSH_HOUR, PUSH_MINUTE)
        
        while True:
            now = datetime.now()
            if now.hour == PUSH_HOUR and now.minute == PUSH_MINUTE:
                run_full_analysis()
                # 避免同一分钟内重复执行
                time.sleep(60)
            time.sleep(30)


if __name__ == "__main__":
    main()
