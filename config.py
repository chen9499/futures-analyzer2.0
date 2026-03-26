# ==================== 系统配置 ====================

# ==================== 推送方式：邮箱 ====================
# QQ邮箱 SMTP 设置（授权码不是QQ密码！）
EMAIL_HOST = "smtp.qq.com"
EMAIL_PORT = 465
EMAIL_USE_SSL = True
EMAIL_USERNAME = "370898687@qq.com"
EMAIL_PASSWORD = "dkrhzmwrbbfqcbaf"   # SMTP 授权码
EMAIL_TO = "370898687@qq.com"          # 收件邮箱

# 推送方式选择：email / serverchan（只能选一个）
PUSH_METHOD = "email"   # "email" 或 "serverchan"

# ==================== LLM配置：Kimi ====================
# 获取地址: https://platform.moonshot.cn/
KIMI_API_KEY = "sk-ajkA6TvV0n4khaK2o0Bh0xF8ktQRqBdXFXUXkDtkwFpDOn8g"
KIMI_BASE_URL = "https://api.moonshot.cn/v1"
MODEL_NAME = "moonshot-v1-8k"   # 免费额度内够用，支持8k上下文

# 关注品种列表（全部国内期货品种分类扫描）
CATEGORIES = {
    "黑色系": ["RB2505", "HC2505", "I2505", "J2505", "JM2505"],
    "有色金属": ["CU2505", "AL2505", "ZN2505", "PB2505", "NI2505", "SN2505"],
    "能化系": ["SC2505", "FU2505", "L2505", "PP2505", "MA2505", "TA2505", "EG2505", "BU2505"],
    "农产品": ["M2505", "Y2505", "RM2505", "OI2505", "P2505", "CS2505", "CF2505", "SR2505", "AP2505"],
    "金融期货": ["IF2505", "IC2505", "IM2505", "IH2505", "T2506", "TS2506"],
}

# 每天推送时间（24小时制，15:30 = 收盘后30分钟，数据完整）
PUSH_HOUR = 15
PUSH_MINUTE = 30

# 是否开启LLM分析（关闭则只推送原始行情数据）
ENABLE_LLM = True
