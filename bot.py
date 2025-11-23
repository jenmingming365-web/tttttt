import logging
import json
import csv
import os
import asyncio
import pytz
import pandas as pd
import threading
import time
import requests
from datetime import datetime, date, time, timedelta
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler
)
from flask import Flask

# === Flask App for Render Health Checks ===
app = Flask(__name__)

@app.route('/')
def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Telegram Shift Bot Status</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
            .container { max-width: 800px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            .status { color: green; font-weight: bold; font-size: 1.2em; }
            .info { background: #e7f3ff; padding: 15px; border-radius: 5px; margin: 15px 0; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1> Telegram Shift Bot Status</h1>
            <p class="status">✅ Bot is running successfully on Render</p>
            <div class="info">
                <p><strong>Last updated:</strong> {}</p>
                <p><strong>Server time:</strong> {}</p>
                <p><strong>Bot timezone:</strong> Asia/Phnom_Penh</p>
                <p><strong>Status:</strong> Operational</p>
            </div>
            <p>This bot manages shift tracking, penalties, and multi-language support for teams.</p>
        </div>
    </body>
    </html>
    """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), get_current_time().strftime("%Y-%m-%d %H:%M:%S"))

@app.route('/health')
def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat(), "service": "telegram-shift-bot"}

@app.route('/ping')
def ping():
    return "pong"

def run_flask():
    """Run Flask app in a separate thread for Render health checks"""
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

# === CONFIG (Using Environment Variables for Security) ===
TOKEN = os.environ.get('BOT_TOKEN', '8146462889:AAFx1i_I0U7xxVsSLQze2JCNkg3Vs-U6Q5g')
OWNER_ID = int(os.environ.get('OWNER_ID', '7953403242'))
GROUP_ID = int(os.environ.get('GROUP_ID', '-1003344849078'))

# Language configuration - can be changed by owner
BOT_CONFIG = {
    "work_start_limit": time(9, 0, 0),
    "work_overtime_limit": time(22, 0, 0),
    "daily_reset_time": time(15, 0, 0),
    "summary_time": time(13, 0, 0),
    "timezone": "Asia/Phnom_Penh",
    "default_language": "khmer",  # Default language for new users
    "require_username": True,  # Require username for adding bot to groups
    "allowed_users": []  # List of user IDs allowed to add bot to groups
}

# === Fines Configuration ===
FINES = {
    "late": 1000,
    "night": 60,
    "overtime": 10
}

# Activity time limits (in minutes)
ACTIVITY_LIMITS = {
    "meal": 45,
    "toilet": 12,
    "smoke": 8
}

# Multi-language Texts (Keep your existing TEXTS dictionary here)
TEXTS = {
    "khmer": {
        "language_name": "ភាសាខ្មែរ",
        "welcome": "សូមស្វាគមន៍មកកាន់ប្រព័ន្ធកំណត់វេន!",
        "bot_status": "ប្រព័ន្ធកំណត់វេន\nស្ថានភាព: បានបើកក្ដារចម្លើយ\n",
        "start_work": "ចាប់ផ្ដើមធ្វើការ",
        "end_work": "បញ្ចប់ធ្វើការ", 
        "meal": "ញ៉ាំអាហារ",
        "toilet": "ទៅបន្ទប់ទឹក",
        "smoke": "ជក់បារី",
        "back": "ត្រឡប់មកវិញ",
        "status": "ស្ថានភាព",
        "success": "✅ កំណត់វេនជោគជ័យ",
        "error": "❌ កំហុស",
        "warning": "⚠️ ព្រមាន",
        "penalty": "🚨 ជូនដំណឹងពិន័យ",
        "stats": "📊 ស្ថិតិ",
        "reset": "🔄 កំណត់ឡើងវិញ",
        "config": "⚙️ ការកំណត់",
        "late_penalty": "ពិន័យមកយឺត",
        "overtime_penalty": "ពិន័យពេលវេលាលើស",
        "night_penalty": "ពិន័យរៀនយប់",
        "free_version": "ប្រព័ន្ធនេះជាកំណែឥតគិតថ្លៃ ប្រសិនបើអ្នកចង់បើកលក្ខណៈពិសេសកំណែបន្តិចទៀត និងទទួលបានជំនួយបច្ចេកទេស សូមទាក់ទងអតិថិជន: t.me/@ttm_xx1",
        "unauthorized_welcome": "👋 សូមស្វាគមន៍មកកាន់ប្រព័ន្ធកំណត់វេន!",
        "unauthorized_detected": "🔍 បានរកឃើញការបន្ថែមក្រុមថ្មី",
        "unauthorized_processing": "⏳ កំពុងដំណើរការចាប់ផ្ដើមក្រុមថ្មី សូមរង់ចាំ...",
        "unauthorized_leaving": "🚫 ការជូនដំណឹង: ប្រព័ន្ធនឹងចាកចេញពីក្រុម",
        "unauthorized_reason": "📋 មូលហេតុ: ការប្រើប្រាស់ដោយគ្មានការអនុញ្ញាត",
        "unauthorized_contact": "📞 សម្រាប់ការអនុញ្ញាត សូមទាក់ទង: @ttm_1214",
        "no_username_error": "❌ អ្នកមិនមានឈ្មោះប្រើប្រាស់ Telegram សូមកំណត់ឈ្មោះប្រើប្រាស់ក្នុងការកំណត់ Telegram របស់អ្នកមុនពេលបន្ថែមបូតទៅក្រុម",
        "not_allowed_error": "❌ អ្នកមិនមានសិទ្ធិបន្ថែមបូតនេះទៅក្រុមទេ",
        "group_approved": "✅ ក្រុមត្រូវបានអនុម័ត",
        "language_set": "✅ ភាសាត្រូវបានកំណត់",
        "meal_count_1": "អាហារលើកទី១",
        "meal_count_2": "អាហារលើកទី២",
        "rest": "ឈប់សម្រាក",
        "set_language": "កំណត់ភាសា",
        "available_languages": "ភាសាដែលមាន",
        "current_language": "ភាសាបច្ចុប្បន្ន",
        "invalid_language": "ភាសាមិនត្រឹមត្រូវ",
        "add_allowed_user": "បន្ថែមអ្នកប្រើប្រាស់ដែលអនុញ្ញាត",
        "enter_user_id": "សូមបញ្ចូល ID អ្នកប្រើប្រាស់:",
        "user_added": "បានបន្ថែមអ្នកប្រើប្រាស់ដោយជោគជ័យ",
        "user_already_added": "អ្នកប្រើប្រាស់មានក្នុងបញ្ជីរួចហើយ",
        "invalid_user_id": "ID អ្នកប្រើប្រាស់មិនត្រឹមត្រូវ",
        "allowed_users": "អ្នកប្រើប្រាស់ដែលអនុញ្ញាត",
        "no_allowed_users": "គ្មានអ្នកប្រើប្រាស់ដែលអនុញ្ញាត",
        "unknown_user": "អ្នកប្រើប្រាស់មិនស្គាល់",
        "no_username": "គ្មានឈ្មោះប្រើប្រាស់",
        "unknown": "មិនស្គាល់",
        "unauthorized_group_addition": "ការព្យាយាមបន្ថែមក្រុមដោយគ្មានការអនុញ្ញាត",
        "bot_will_leave": "បូតនឹងចាកចេញពីក្រុមដោយស្វ័យប្រវត្តិ",
        "failed_to_leave_group": "បរាជ័យក្នុងការចាកចេញពីក្រុមដោយគ្មានការអនុញ្ញាត",
        "please_remove_manually": "សូមយកបូតចេញដោយខ្លួនឯង",
        "personal_status": "ស្ថានភាពផ្ទាល់ខ្លួន",
        "working_on": "កំពុងធ្វើការ",
        "none": "គ្មាន",
        "user": "អ្នកប្រើប្រាស់",
        "user_id": "អត្តសញ្ញាណអ្នកប្រើប្រាស់",
        "current_activity": "សកម្មភាពបច្ចុប្បន្ន",
        "work_today": "ធ្វើការថ្ងៃនេះ",
        "penalty_today": "ពិន័យថ្ងៃនេះ",
        "meal_count": "ចំនួនអាហារ",
        "toilet_count": "ចំនួនទៅបន្ទប់ទឹក",
        "times": " ដង",
        "currency": "៛",
        "reason": "មូលហេតុ",
        "already_started_work": "អ្នកបានចាប់ផ្ដើមធ្វើការរួចហើយ",
        "late_arrival": "អ្នកមកយឺត",
        "late_duration": "ពេលវេលាយឺត",
        "info": "ព័ត៌មាន",
        "late_recorded": "ការមកយឺតនេះត្រូវបានកត់ត្រា",
        "late_penalty_this_time": "ពិន័យមកយឺតលើកនេះ",
        "good_morning": "អរុណសួស្ដី",
        "night_shift_after_10pm": "កំណត់វេនយប់ក្រោយម៉ោង 22:00",
        "return_from_break": "ត្រឡប់មកពីសកម្មភាព",
        "activity_time_used": "ពេលវេលាប្រើប្រាស់សកម្មភាពនេះ",
        "total_activity_time": "ពេលវេលាសរុបសកម្មភាពនេះ",
        "total_active_time": "ពេលវេលាសកម្មសរុបថ្ងៃនេះ",
        "system_started": "ប្រព័ន្ធបានចាប់ផ្ដើម",
        "change_language": "ប្ដូរភាសា",
        "language_updated": "ភាសាត្រូវបានផ្លាស់ប្ដូរ",
        "your_language": "ភាសារបស់អ្នក",
        "choose_language": "សូមជ្រើសរើសភាសារបស់អ្នក:",
        "language_help": "ប្រើ /km, /ch, /en ដើម្បីប្ដូរភាសាផ្ទាល់ខ្លួនរបស់អ្នក"
    },
    "english": {
        "language_name": "English",
        "welcome": "Welcome to the shift management system!",
        "bot_status": "Shift Management System\nStatus: Keyboard enabled\n",
        "start_work": "Start Work",
        "end_work": "End Work", 
        "meal": "Meal Break",
        "toilet": "Toilet Break",
        "smoke": "Smoke Break",
        "back": "Return Back",
        "status": "Status",
        "success": "✅ Check-in successful",
        "error": "❌ Error",
        "warning": "⚠️ Warning",
        "penalty": "🚨 Penalty Notice",
        "stats": "📊 Statistics",
        "reset": "🔄 Reset",
        "config": "⚙️ Configuration",
        "late_penalty": "Late penalty",
        "overtime_penalty": "Overtime penalty",
        "night_penalty": "Night shift penalty",
        "free_version": "This is a free version. If you want to unlock more features and get technical support, please contact customer service: t.me/@ttm_xx1",
        "unauthorized_welcome": "👋 Welcome to the shift management system!",
        "unauthorized_detected": "🔍 New group addition detected",
        "unauthorized_processing": "⏳ Processing new group initialization, please wait...",
        "unauthorized_leaving": "🚫 Notification: System will leave the group",
        "unauthorized_reason": "📋 Reason: Unauthorized usage",
        "unauthorized_contact": "📞 For authorization, please contact: @ttm_1214",
        "no_username_error": "❌ You don't have a Telegram username. Please set a username in your Telegram settings before adding this bot to groups",
        "not_allowed_error": "❌ You are not allowed to add this bot to groups",
        "group_approved": "✅ Group approved",
        "language_set": "✅ Language set",
        "meal_count_1": "Meal 1st time",
        "meal_count_2": "Meal 2nd time",
        "rest": "Rest Break",
        "set_language": "Set Language",
        "available_languages": "Available languages",
        "current_language": "Current language",
        "invalid_language": "Invalid language",
        "add_allowed_user": "Add Allowed User",
        "enter_user_id": "Please enter user ID:",
        "user_added": "User added successfully",
        "user_already_added": "User already in allowed list",
        "invalid_user_id": "Invalid user ID",
        "allowed_users": "Allowed Users",
        "no_allowed_users": "No allowed users",
        "unknown_user": "Unknown User",
        "no_username": "No username",
        "unknown": "Unknown",
        "unauthorized_group_addition": "Unauthorized group addition attempt",
        "bot_will_leave": "Bot will leave the group automatically",
        "failed_to_leave_group": "Failed to leave unauthorized group",
        "please_remove_manually": "Please remove the bot manually",
        "personal_status": "Personal Status",
        "working_on": "Currently working on",
        "none": "None",
        "user": "User",
        "user_id": "User ID",
        "current_activity": "Current Activity",
        "work_today": "Work today",
        "penalty_today": "Penalty today",
        "meal_count": "Meal count",
        "toilet_count": "Toilet count",
        "times": " times",
        "currency": "៛",
        "reason": "Reason",
        "already_started_work": "You have already started work",
        "late_arrival": "You are late",
        "late_duration": "Late duration",
        "info": "Information",
        "late_recorded": "This late arrival has been recorded",
        "late_penalty_this_time": "Late penalty this time",
        "good_morning": "Good morning",
        "night_shift_after_10pm": "Night shift after 10:00 PM",
        "return_from_break": "Return from activity",
        "activity_time_used": "Time used for this activity",
        "total_activity_time": "Total time for this activity",
        "total_active_time": "Total active time today",
        "system_started": "System started",
        "change_language": "Change Language",
        "language_updated": "Language updated",
        "your_language": "Your language",
        "choose_language": "Please choose your language:",
        "language_help": "Use /km, /ch, /en to change your personal language"
    },
    "chinese": {
        "language_name": "中文",
        "welcome": "欢迎使用班次管理系统！",
        "bot_status": "班次管理系统\n状态：键盘已启用\n",
        "start_work": "上班",
        "end_work": "下班", 
        "meal": "餐",
        "toilet": "上厕所",
        "smoke": "抽烟",
        "back": "回座",
        "status": "状态",
        "success": "✅ 打卡成功",
        "error": "❌ 错误",
        "warning": "⚠️ 警告",
        "penalty": "🚨 处罚通知",
        "stats": "📊 统计",
        "reset": "🔄 重置",
        "config": "⚙️ 配置",
        "late_penalty": "迟到处罚",
        "overtime_penalty": "超时处罚",
        "night_penalty": "夜班处罚",
        "free_version": "这是免费版本。如果您想解锁更多功能并获得技术支持，请联系客服：t.me/@ttm_xx1",
        "unauthorized_welcome": "👋 欢迎使用班次管理系统！",
        "unauthorized_detected": "🔍 检测到新群组添加",
        "unauthorized_processing": "⏳ 正在处理新群组初始化，请稍候...",
        "unauthorized_leaving": "🚫 通知：系统将离开群组",
        "unauthorized_reason": "📋 原因：未经授权使用",
        "unauthorized_contact": "📞 如需授权，请联系：@ttm_1214",
        "no_username_error": "❌ 您没有Telegram用户名。请在将机器人添加到群组之前在Telegram设置中设置用户名",
        "not_allowed_error": "❌ 您无权将此机器人添加到群组",
        "group_approved": "✅ 群组已批准",
        "language_set": "✅ 语言已设置",
        "meal_count_1": "第一次用餐",
        "meal_count_2": "第二次用餐",
        "rest": "休息时间",
        "set_language": "设置语言",
        "available_languages": "可用语言",
        "current_language": "当前语言",
        "invalid_language": "无效语言",
        "add_allowed_user": "添加允许用户",
        "enter_user_id": "请输入用户ID：",
        "user_added": "用户添加成功",
        "user_already_added": "用户已在允许列表中",
        "invalid_user_id": "无效用户ID",
        "allowed_users": "允许用户",
        "no_allowed_users": "没有允许用户",
        "unknown_user": "未知用户",
        "no_username": "无用户名",
        "unknown": "未知",
        "unauthorized_group_addition": "未经授权的群组添加尝试",
        "bot_will_leave": "机器人将自动离开群组",
        "failed_to_leave_group": "无法离开未经授权的群组",
        "please_remove_manually": "请手动移除机器人",
        "personal_status": "个人状态",
        "working_on": "当前工作",
        "none": "无",
        "user": "用户",
        "user_id": "用户ID",
        "current_activity": "当前活动",
        "work_today": "今日工作",
        "penalty_today": "今日处罚",
        "meal_count": "用餐次数",
        "toilet_count": "卫生间次数",
        "times": " 次",
        "currency": "៛",
        "reason": "原因",
        "already_started_work": "您已经开始工作",
        "late_arrival": "您迟到了",
        "late_duration": "迟到时间",
        "info": "信息",
        "late_recorded": "此次迟到已被记录",
        "late_penalty_this_time": "此次迟到处罚",
        "good_morning": "早上好",
        "night_shift_after_10pm": "晚上10点后的夜班",
        "return_from_break": "从活动中返回",
        "activity_time_used": "此活动使用时间",
        "total_activity_time": "此活动总时间",
        "total_active_time": "今日总活动时间",
        "system_started": "系统已启动",
        "change_language": "更改语言",
        "language_updated": "语言已更新",
        "your_language": "您的语言",
        "choose_language": "请选择您的语言：",
        "language_help": "使用 /km, /ch, /en 更改您的个人语言"
    }
}

# Store individual user languages
user_languages = {}

def get_user_language(user_id: int) -> str:
    """Get user's preferred language, fallback to default if not set"""
    return user_languages.get(user_id, BOT_CONFIG["default_language"])

def get_text(key, user_id: int = None):
    """Get text in user's preferred language"""
    if user_id is None:
        # For system messages, use default language
        lang = BOT_CONFIG["default_language"]
    else:
        lang = get_user_language(user_id)
    return TEXTS[lang].get(key, key)

def get_keyboard(user_id: int):
    """Get keyboard in user's preferred language"""
    lang = get_user_language(user_id)
    texts = TEXTS[lang]
    reply_keyboard = [
        [texts["start_work"], texts["end_work"]],
        [texts["meal"], texts["toilet"], texts["smoke"]],
        [texts["back"], texts["status"]]
    ]
    return ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)

def get_admin_keyboard(user_id: int):
    """Get admin keyboard in user's preferred language"""
    lang = get_user_language(user_id)
    texts = TEXTS[lang]
    admin_keyboard = [
        ["/stats", "/export", "/broadcast"],
        ["/approve_group", "/list_groups", "/unauthorized_logs"],
        ["/set_work_time", "/set_reset_time", "/show_config"],
        ["/set_language", "/add_allowed_user", "/list_allowed_users"]
    ]
    return ReplyKeyboardMarkup(admin_keyboard, resize_keyboard=True)

# Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Data storage
user_records = {}
approved_groups = set()
user_warnings = {}
unauthorized_attempts = []
group_members = {}

# Conversation states
APPROVE_GROUP = 1
SET_WORK_TIME = 2
SET_RESET_TIME = 3
SET_LANGUAGE = 4
ADD_ALLOWED_USER = 5

def is_owner(user_id: int) -> bool:
    """Check if user is the owner or authorized admin"""
    authorized_users = [OWNER_ID, 7953403242, 123456789, 987654321]
    return user_id in authorized_users

def is_allowed_user(user_id: int) -> bool:
    """Check if user is allowed to add bot to groups"""
    return user_id in BOT_CONFIG["allowed_users"] or is_owner(user_id)

def has_username(user) -> bool:
    """Check if user has username"""
    return user.username is not None and user.username.strip() != ""

async def is_approved_group(chat_id: int) -> bool:
    """Check if group is approved"""
    return chat_id in approved_groups or chat_id == GROUP_ID

def get_current_time():
    """Get current time in configured timezone"""
    try:
        tz = pytz.timezone(BOT_CONFIG["timezone"])
        return datetime.now(tz)
    except:
        return datetime.now()

def make_timezone_aware(dt):
    """Make a datetime timezone aware"""
    if dt.tzinfo is None:
        tz = pytz.timezone(BOT_CONFIG["timezone"])
        return tz.localize(dt)
    return dt

async def send_status_message(chat_id: int, bot, additional_text: str = ""):
    """Send status message to a specific group (uses default language)"""
    total_members = len(group_members.get(chat_id, {}))
    active_today = sum(1 for uid, record in user_records.items() 
                      if record.get("work_start") or record.get("penalties"))
    
    status_msg = (
        f"{get_text('bot_status', None)}"
        f"{additional_text}"
        "——————————————\n"
        f"{get_text('free_version', None)}"
    )
    
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=status_msg,
            reply_markup=get_keyboard(OWNER_ID)  # Use owner's language for group messages
        )
        logger.info(f"Sent status message to group {chat_id}")
    except Exception as e:
        logger.error(f"Failed to send status message to {chat_id}: {e}")

async def send_startup_message(context: ContextTypes.DEFAULT_TYPE):
    """Send startup message to all approved groups"""
    for group_id in approved_groups:
        await send_status_message(group_id, context.bot, "✅ " + get_text("system_started", None) + "\n")

async def get_user_detailed_info(user):
    """Get detailed information about a user"""
    try:
        user_info = {
            "id": user.id,
            "first_name": user.first_name,
            "last_name": user.last_name if user.last_name else get_text("none", user.id),
            "username": f"@{user.username}" if user.username else get_text("no_username", user.id),
            "language_code": user.language_code if user.language_code else get_text("unknown", user.id),
            "is_bot": user.is_bot,
            "is_premium": getattr(user, 'is_premium', False),
            "added_date": get_current_time().strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp": get_current_time().isoformat(),
            "preferred_language": get_user_language(user.id)
        }
        return user_info
    except Exception as e:
        logger.error(f"Error getting user info: {e}")
        return {"error": str(e)}

async def update_group_members(chat_id: int, bot):
    """Update group members tracking"""
    try:
        if chat_id not in group_members:
            group_members[chat_id] = {}
        
        try:
            chat = await bot.get_chat(chat_id)
            logger.info(f"Group {chat_id} info updated: {chat.title}")
        except Exception as e:
            logger.warning(f"Could not get chat info for {chat_id}: {e}")
        
        logger.info(f"Group members tracking initialized for {chat_id}")
        
    except Exception as e:
        logger.error(f"Failed to update group members for {chat_id}: {e}")
        if chat_id not in group_members:
            group_members[chat_id] = {}

async def track_user_activity(chat_id: int, user):
    """Track user activity in group members"""
    if chat_id not in group_members:
        group_members[chat_id] = {}
    
    user_id = user.id
    if user_id not in group_members[chat_id]:
        group_members[chat_id][user_id] = {
            "user": user,
            "first_seen": get_current_time(),
            "last_active": get_current_time(),
            "message_count": 1,
            "language": get_user_language(user_id)
        }
    else:
        group_members[chat_id][user_id]["last_active"] = get_current_time()
        group_members[chat_id][user_id]["message_count"] += 1
        group_members[chat_id][user_id]["language"] = get_user_language(user_id)

async def save_backup_files():
    """Save backup files in multiple formats"""
    current_time = get_current_time()
    timestamp = current_time.strftime("%Y%m%d_%H%M%S")
    
    backup_data = {
        "timestamp": timestamp,
        "user_records": user_records,
        "approved_groups": list(approved_groups),
        "unauthorized_attempts": unauthorized_attempts,
        "bot_config": BOT_CONFIG,
        "group_members": group_members,
        "user_languages": user_languages
    }
    
    # Ensure backup directory exists
    os.makedirs("backups", exist_ok=True)
    
    # JSON backup
    json_filename = f"backups/backup_{timestamp}.json"
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(backup_data, f, ensure_ascii=False, indent=2, default=str)
    
    # CSV backup for user records
    csv_filename = f"backups/user_records_{timestamp}.csv"
    with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['User ID', 'User Name', 'Work Total', 'Total Penalties', 'Last Reset', 'Language'])
        
        for uid, record in user_records.items():
            total_penalty = 0
            if record.get("penalties"):
                for p in record["penalties"]:
                    try:
                        if "៛" in p:
                            penalty_amount = int(p.split("៛")[0].split()[-1])
                            total_penalty += penalty_amount
                    except (ValueError, IndexError):
                        continue
            
            writer.writerow([
                uid, 
                record.get("name", ""),
                record.get("work_total", 0), 
                total_penalty,
                record.get("last_reset", ""),
                get_user_language(uid)
            ])
    
    # Excel backup for user records
    excel_filename = f"backups/user_records_{timestamp}.xlsx"
    
    # Prepare data for Excel
    excel_data = []
    for uid, record in user_records.items():
        total_penalty = 0
        penalty_details = []
        if record.get("penalties"):
            for p in record["penalties"]:
                if "៛" in p:
                    try:
                        penalty_amount = int(p.split("៛")[0].split()[-1])
                        total_penalty += penalty_amount
                        penalty_details.append(p)
                    except (ValueError, IndexError):
                        continue
        
        pure_work_time = record.get("work_total", 0) - record["times"].get("total", 0)
        if pure_work_time < 0:
            pure_work_time = 0
        
        excel_data.append({
            'User ID': uid,
            'User Name': record.get("name", ""),
            'Work Total Seconds': record.get("work_total", 0),
            'Work Total': format_duration(record.get("work_total", 0)),
            'Pure Work Seconds': pure_work_time,
            'Pure Work Time': format_duration(pure_work_time),
            'Total Penalties': total_penalty,
            'Penalty Details': '; '.join(penalty_details) if penalty_details else 'None',
            'Meal Count': record['counts'].get(get_text("meal", uid), 0),
            'Toilet Count': record['counts'].get(get_text("toilet", uid), 0),
            'Smoke Count': record['counts'].get(get_text("smoke", uid), 0),
            'Rest Count': record['counts'].get(get_text("rest", uid), 0),
            'Meal 1 Count': record['counts'].get(get_text("meal_count_1", uid), 0),
            'Meal 2 Count': record['counts'].get(get_text("meal_count_2", uid), 0),
            'Total Active Seconds': record['times'].get('total', 0),
            'Total Active Time': format_duration(record['times'].get('total', 0)),
            'Last Reset': record.get("last_reset", ""),
            'Language': get_user_language(uid)
        })
    
    # Create DataFrame and save to Excel
    if excel_data:
        df = pd.DataFrame(excel_data)
        df.to_excel(excel_filename, index=False, engine='openpyxl')
    
    return json_filename, csv_filename, excel_filename

async def send_backup_to_group(context: ContextTypes.DEFAULT_TYPE):
    """Send backup files to the group"""
    try:
        json_file, csv_file, excel_file = await save_backup_files()
        current_time = get_current_time()
        
        # Send JSON backup
        with open(json_file, 'rb') as f:
            await context.bot.send_document(
                chat_id=GROUP_ID,
                document=f,
                caption=f"📊 Daily Data Backup (JSON)\nTime: {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        
        # Send CSV backup
        with open(csv_file, 'rb') as f:
            await context.bot.send_document(
                chat_id=GROUP_ID,
                document=f,
                caption=f"📋 User Records Backup (CSV)\nTime: {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        
        # Send Excel backup
        with open(excel_file, 'rb') as f:
            await context.bot.send_document(
                chat_id=GROUP_ID,
                document=f,
                caption=f"📈 Detailed User Report (Excel)\nTime: {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        
        # Cleanup
        try:
            os.remove(json_file)
            os.remove(csv_file)
            os.remove(excel_file)
        except:
            pass
        
        logger.info("Backup files sent successfully")
        
    except Exception as e:
        logger.error(f"Failed to send backup: {e}")
        await context.bot.send_message(
            chat_id=GROUP_ID,
            text=f"❌ Failed to send backup files: {e}"
        )

async def reset_all_records(context: ContextTypes.DEFAULT_TYPE):
    """Reset all user records and send backup"""
    global user_records
    
    # Send backup before reset
    await send_backup_to_group(context)
    
    # Reset records but keep user names
    reset_records = {}
    for uid, record in user_records.items():
        reset_records[uid] = {
            "counts": {
                get_text("meal", uid): 0, 
                get_text("toilet", uid): 0, 
                get_text("smoke", uid): 0, 
                get_text("rest", uid): 0, 
                get_text("meal_count_1", uid): 0, 
                get_text("meal_count_2", uid): 0
            },
            "times": {
                get_text("meal", uid): 0, 
                get_text("toilet", uid): 0, 
                get_text("smoke", uid): 0, 
                get_text("rest", uid): 0, 
                get_text("meal_count_1", uid): 0, 
                get_text("meal_count_2", uid): 0, 
                "total": 0
            },
            "active": None,
            "work_start": None, 
            "work_total": 0,
            "penalties": [], 
            "last_reset": get_current_time().date(),
            "name": record.get("name", "")
        }
    
    user_records = reset_records
    user_warnings.clear()
    
    current_time = get_current_time()
    logger.info(f"Daily reset done at {current_time}")
    
    # Send reset notification to all approved groups
    for group_id in approved_groups:
        await send_status_message(
            group_id, 
            context.bot,
            f"🔄 Daily data reset completed!\nTime: {current_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        )

def reset_if_new_day(uid):
    """Reset user record if it's a new day"""
    today = get_current_time().date()
    if uid not in user_records:
        user_records[uid] = {
            "counts": {
                get_text("meal", uid): 0, 
                get_text("toilet", uid): 0, 
                get_text("smoke", uid): 0, 
                get_text("rest", uid): 0, 
                get_text("meal_count_1", uid): 0, 
                get_text("meal_count_2", uid): 0
            },
            "times": {
                get_text("meal", uid): 0, 
                get_text("toilet", uid): 0, 
                get_text("smoke", uid): 0, 
                get_text("rest", uid): 0, 
                get_text("meal_count_1", uid): 0, 
                get_text("meal_count_2", uid): 0, 
                "total": 0
            },
            "active": None,
            "work_start": None, 
            "work_total": 0,
            "penalties": [], 
            "last_reset": today,
            "name": ""
        }
    elif user_records[uid].get("last_reset") != today:
        user_name = user_records[uid].get("name", "")
        user_records[uid] = {
            "counts": {
                get_text("meal", uid): 0, 
                get_text("toilet", uid): 0, 
                get_text("smoke", uid): 0, 
                get_text("rest", uid): 0, 
                get_text("meal_count_1", uid): 0, 
                get_text("meal_count_2", uid): 0
            },
            "times": {
                get_text("meal", uid): 0, 
                get_text("toilet", uid): 0, 
                get_text("smoke", uid): 0, 
                get_text("rest", uid): 0, 
                get_text("meal_count_1", uid): 0, 
                get_text("meal_count_2", uid): 0, 
                "total": 0
            },
            "active": None,
            "work_start": None, 
            "work_total": 0,
            "penalties": [], 
            "last_reset": today,
            "name": user_name
        }

def format_duration(seconds: int):
    """Format seconds into human readable duration"""
    if seconds < 0:
        seconds = 0
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h:02d}h {m:02d}m {s:02d}s"
    elif m > 0:
        return f"{m:02d}m {s:02d}s"
    else:
        return f"{s:02d}s"

async def send_penalty(context, user, action, status, fine, overtime=None):
    """Announce penalty in group, with @mention"""
    mention = f"[{user.first_name}](tg://user?id={user.id})"
    msg = (
        f"🚨 {get_text('penalty', None)}\n"
        f"{get_text('user', None)}: {mention}\n"
        f"Action: {action}\n"
        f"Status: {status}\n"
    )
    if overtime:
        msg += f"Overtime: {format_duration(overtime)}\n"
    msg += f"Penalty Amount: {fine}{get_text('currency', None)}"

    await context.bot.send_message(chat_id=GROUP_ID, text=msg, parse_mode="Markdown")

# === Keep Alive Function for Render Free Tier ===
def keep_alive():
    """Ping the service to prevent sleep on free tier"""
    try:
        render_service_url = os.environ.get('RENDER_SERVICE_URL', '')
        if render_service_url:
            response = requests.get(f"{render_service_url}/ping", timeout=10)
            logger.info(f"✅ Keep-alive ping successful: {response.status_code}")
        else:
            logger.info("ℹ️ Keep-alive: No RENDER_SERVICE_URL set, using internal pinging")
            # Internal ping to keep the process alive
            requests.get('http://localhost:5000/ping', timeout=5)
    except Exception as e:
        logger.warning(f"⚠️ Keep-alive ping failed: {e}")
    
    # Schedule next ping in 8 minutes (480 seconds) to stay under 15-minute timeout
    threading.Timer(480, keep_alive).start()

# === Broadcast Penalties Function ===
async def broadcast_penalties(context: ContextTypes.DEFAULT_TYPE):
    """Send a daily summary of penalties to the group."""
    if not user_records:
        await context.bot.send_message(chat_id=GROUP_ID, text="📊 No shift records today")
        return

    today = get_current_time().strftime("%Y-%m-%d")
    lines = [f"📊 Daily Penalty Summary ({today})"]

    total_group_penalty = 0

    for uid, record in user_records.items():
        total_penalty = 0
        user_penalties = []
        
        for p in record.get("penalties", []):
            if "៛" in p:
                try:
                    penalty_amount = int(p.split("៛")[0].split()[-1])
                    total_penalty += penalty_amount
                    user_penalties.append(p)
                except (ValueError, IndexError):
                    continue
        
        total_group_penalty += total_penalty

        user_name = record.get("name", f"User{uid}")
        if total_penalty == 0:
            lines.append(f"\n👤 {user_name}\nTotal Penalty: 0{get_text('currency', None)} ✅")
        else:
            details = "\n".join(user_penalties)
            lines.append(f"\n👤 {user_name}\nTotal Penalty: {total_penalty}{get_text('currency', None)}\nDetails:\n{details}")

    lines.append(f"\n💰 Total Group Penalty: {total_group_penalty}{get_text('currency', None)}")
    msg = "\n".join(lines)
    await context.bot.send_message(chat_id=GROUP_ID, text=msg, parse_mode="Markdown")

# === Activity Warnings Function ===
async def check_activity_warnings(context: ContextTypes.DEFAULT_TYPE):
    """Check for activity warnings (2 minutes before timeout)"""
    now = get_current_time()
    for uid, record in user_records.items():
        if record.get("active"):
            action, start_time = record["active"]
            duration = (now - start_time).total_seconds()
            
            if action in [get_text("meal_count_1", uid), get_text("meal_count_2", uid)]:
                base_action = get_text("meal", uid)
            else:
                base_action = action
                
            if base_action in ACTIVITY_LIMITS:
                time_limit = ACTIVITY_LIMITS[base_action] * 60
                time_remaining = time_limit - duration
                
                if 120 >= time_remaining > 0 and not user_warnings.get(uid):
                    try:
                        user = await context.bot.get_chat(uid)
                        warning_msg = (
                            f"{get_text('user', None)}: {user.first_name}\n"
                            f"{get_text('user_id', None)}: {user.id}\n"
                            f"⚠️ {get_text('warning', None)}: Your {action} activity will timeout in 2 minutes, please return soon!\n"
                            f"Return: /back\n"
                            f"{get_text('free_version', None)}"
                        )
                        await context.bot.send_message(chat_id=GROUP_ID, text=warning_msg)
                        user_warnings[uid] = True
                        logger.info(f"Sent warning to user {uid} for {action}")
                    except Exception as e:
                        logger.error(f"Failed to send warning for {uid}: {e}")

# === Activity Overtime Function ===
async def check_activity_overtime(context: ContextTypes.DEFAULT_TYPE):
    """Check for activity overtime and apply penalties"""
    now = get_current_time()
    for uid, record in user_records.items():
        if record.get("active"):
            action, start_time = record["active"]
            duration = (now - start_time).total_seconds()
            
            if action in [get_text("meal_count_1", uid), get_text("meal_count_2", uid)]:
                base_action = get_text("meal", uid)
            else:
                base_action = action
            
            if base_action in ACTIVITY_LIMITS and duration > ACTIVITY_LIMITS[base_action] * 60:
                fine = FINES["overtime"]
                penalty_text = f"{action} overtime penalty {fine}{get_text('currency', uid)}"
                
                penalty_applied = any(penalty_text in p for p in record.get("penalties", []))
                
                if not penalty_applied:
                    record.setdefault("penalties", []).append(penalty_text)
                    
                    try:
                        user = await context.bot.get_chat(uid)
                        await send_penalty(context, user, action, "Activity overtime", fine, int(duration))
                        logger.info(f"Applied overtime penalty to user {uid} for {action}")
                    except Exception as e:
                        logger.error(f"Failed to get user info for {uid}: {e}")

# === Configuration Commands ===
async def set_work_time_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start work time configuration (Owner only)"""
    user_id = update.message.from_user.id
    if not is_owner(user_id):
        await update.message.reply_text("❌ " + get_text("not_allowed_error", user_id))
        return ConversationHandler.END
        
    await update.message.reply_text(
        "🕐 Set work start limit time\n"
        "Please enter time (format: HH:MM example 09:00):"
    )
    return SET_WORK_TIME

async def set_work_time_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """End work time configuration"""
    user_id = update.message.from_user.id
    try:
        time_str = update.message.text.strip()
        hours, minutes = map(int, time_str.split(':'))
        new_time = time(hours, minutes, 0)
        
        BOT_CONFIG["work_start_limit"] = new_time
        
        await update.message.reply_text(
            f"✅ Work start limit time updated\n"
            f"New late limit: {new_time.strftime('%H:%M')}\n"
            f"Working after this time will be considered late"
        )
        
    except (ValueError, Exception) as e:
        await update.message.reply_text(f"❌ Invalid time format, please use HH:MM format")
    
    return ConversationHandler.END

async def set_reset_time_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start reset time configuration (Owner only)"""
    user_id = update.message.from_user.id
    if not is_owner(user_id):
        await update.message.reply_text("❌ " + get_text("not_allowed_error", user_id))
        return ConversationHandler.END
        
    await update.message.reply_text(
        "🔄 Set daily reset time\n"
        "Please enter time (format: HH:MM example 15:00):"
    )
    return SET_RESET_TIME

async def set_reset_time_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """End reset time configuration"""
    user_id = update.message.from_user.id
    try:
        time_str = update.message.text.strip()
        hours, minutes = map(int, time_str.split(':'))
        new_time = time(hours, minutes, 0)
        
        BOT_CONFIG["daily_reset_time"] = new_time
        
        # Reschedule the job
        for job in context.application.job_queue.jobs():
            if job.name == "reset_all_records":
                job.schedule_removal()
        
        context.application.job_queue.run_daily(
            reset_all_records, 
            time=new_time
        )
        
        await update.message.reply_text(
            f"✅ Daily reset time updated\n"
            f"New reset time: {new_time.strftime('%H:%M')}\n"
            f"System will automatically reset data at this time every day"
        )
        
    except (ValueError, Exception) as e:
        await update.message.reply_text(f"❌ Invalid time format, please use HH:MM format")
    
    return ConversationHandler.END

async def show_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current bot configuration (Owner only)"""
    user_id = update.message.from_user.id
    if not is_owner(user_id):
        await update.message.reply_text("❌ " + get_text("not_allowed_error", user_id))
        return

    config_msg = (
        "⚙️ Current System Configuration\n"
        "——————————————\n"
        f"🕐 Work start limit: {BOT_CONFIG['work_start_limit'].strftime('%H:%M')}\n"
        f"🌙 Night work limit: {BOT_CONFIG['work_overtime_limit'].strftime('%H:%M')}\n"
        f"🔄 Daily reset time: {BOT_CONFIG['daily_reset_time'].strftime('%H:%M')}\n"
        f"📊 Summary time: {BOT_CONFIG['summary_time'].strftime('%H:%M')}\n"
        f"🌍 System timezone: {BOT_CONFIG['timezone']}\n"
        f"🗣️ Default language: {TEXTS[BOT_CONFIG['default_language']]['language_name']}\n"
        f"👤 Require username: {BOT_CONFIG['require_username']}\n"
        f"✅ Allowed users: {len(BOT_CONFIG['allowed_users'])}\n"
        "——————————————\n"
        "Use /set_work_time or /set_reset_time to edit settings"
    )
    
    await update.message.reply_text(config_msg, reply_markup=get_admin_keyboard(user_id))

# === Admin Commands ===
async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show statistics (Owner only)"""
    user_id = update.message.from_user.id
    if not is_owner(user_id):
        await update.message.reply_text("❌ " + get_text("not_allowed_error", user_id))
        return

    total_users = len(user_records)
    active_users = sum(1 for record in user_records.values() if record.get("work_start") or record.get("penalties"))
    
    total_members = sum(len(members) for members in group_members.values())
    
    total_penalties = 0
    for record in user_records.values():
        for p in record.get("penalties", []):
            if "៛" in p:
                try:
                    penalty_amount = int(p.split("៛")[0].split()[-1])
                    total_penalties += penalty_amount
                except (ValueError, IndexError):
                    continue

    current_time = get_current_time()
    next_reset = datetime.combine(current_time.date(), BOT_CONFIG["daily_reset_time"])
    if current_time.time() > BOT_CONFIG["daily_reset_time"]:
        next_reset += timedelta(days=1)
    
    time_until_reset = next_reset - current_time
    hours, remainder = divmod(int(time_until_reset.total_seconds()), 3600)
    minutes = remainder // 60

    msg = (
        f"📊 System Statistics\n"
        f"Total users: {total_users}\n"
        f"Active users: {active_users}\n"
        f"Tracked members: {total_members}\n"
        f"Total penalties: {total_penalties}{get_text('currency', user_id)}\n"
        f"Approved groups: {len(approved_groups)}\n"
        f"Unauthorized attempts: {len(unauthorized_attempts)} times\n"
        f"Next reset: in {hours}h{minutes}m"
    )
    await update.message.reply_text(msg, reply_markup=get_admin_keyboard(user_id))

async def export_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Export data (Owner only)"""
    user_id = update.message.from_user.id
    if not is_owner(user_id):
        await update.message.reply_text("❌ " + get_text("not_allowed_error", user_id))
        return

    try:
        json_file, csv_file, excel_file = await save_backup_files()
        
        with open(json_file, 'rb') as f:
            await update.message.reply_document(
                document=f,
                caption="JSON Backup File"
            )
        
        with open(csv_file, 'rb') as f:
            await update.message.reply_document(
                document=f,
                caption="CSV Backup File"
            )
        
        with open(excel_file, 'rb') as f:
            await update.message.reply_document(
                document=f,
                caption="Detailed Excel Report"
            )
        
        # Cleanup
        try:
            os.remove(json_file)
            os.remove(csv_file)
            os.remove(excel_file)
        except:
            pass
        
    except Exception as e:
        await update.message.reply_text(f"❌ Export failed: {e}")

async def list_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List approved groups (Owner only)"""
    user_id = update.message.from_user.id
    if not is_owner(user_id):
        await update.message.reply_text("❌ " + get_text("not_allowed_error", user_id))
        return

    if not approved_groups:
        await update.message.reply_text("No approved groups")
        return

    groups_info = ["✅ Approved Groups:"]
    for group_id in approved_groups:
        try:
            chat = await context.bot.get_chat(group_id)
            member_count = len(group_members.get(group_id, {}))
            groups_info.append(f"- {chat.title} (ID: {group_id}, Tracked members: {member_count})")
        except Exception as e:
            groups_info.append(f"- Unknown group (ID: {group_id}) - Error: {e}")

    await update.message.reply_text("\n".join(groups_info))

async def unauthorized_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show unauthorized access attempts (Owner only)"""
    user_id = update.message.from_user.id
    if not is_owner(user_id):
        await update.message.reply_text("❌ " + get_text("not_allowed_error", user_id))
        return

    if not unauthorized_attempts:
        await update.message.reply_text("No unauthorized attempt records")
        return

    logs_info = ["🚫 Unauthorized Attempt Records:"]
    for i, attempt in enumerate(unauthorized_attempts[-10:], 1):
        logs_info.append(
            f"{i}. User: {attempt.get('user_name', 'Unknown')} "
            f"(ID: {attempt.get('user_id', 'Unknown')})\n"
            f"   Group: {attempt.get('group_name', 'Unknown')}\n"
            f"   Time: {attempt.get('timestamp', 'Unknown')}\n"
            f"   Language: {attempt.get('language_code', 'Unknown')}"
        )

    await update.message.reply_text("\n".join(logs_info))

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast message to all approved groups (Owner only)"""
    user_id = update.message.from_user.id
    if not is_owner(user_id):
        await update.message.reply_text("❌ " + get_text("not_allowed_error", user_id))
        return

    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return

    message = " ".join(context.args)
    success_count = 0
    
    for group_id in approved_groups:
        try:
            await context.bot.send_message(chat_id=group_id, text=message)
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to send broadcast to {group_id}: {e}")

    await update.message.reply_text(f"Broadcast completed: {success_count}/{len(approved_groups)} groups")

# === Group Management ===
async def group_approval_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start group approval process (Owner only)"""
    user_id = update.message.from_user.id
    if not is_owner(user_id):
        await update.message.reply_text("❌ " + get_text("not_allowed_error", user_id))
        return ConversationHandler.END
        
    await update.message.reply_text("Please forward a message from the target group or enter group ID:")
    return APPROVE_GROUP

async def group_approval_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """End group approval process"""
    user_id = update.message.from_user.id
    try:
        if update.message.forward_from_chat:
            group_id = update.message.forward_from_chat.id
            group_name = update.message.forward_from_chat.title
        else:
            group_id = int(update.message.text)
            chat = await context.bot.get_chat(group_id)
            group_name = chat.title
            
        approved_groups.add(group_id)
        await update_group_members(group_id, context.bot)
        await update.message.reply_text(f"✅ Approved group: {group_name} (ID: {group_id}")
        
    except (ValueError, Exception) as e:
        await update.message.reply_text(f"❌ Invalid group: {e}")
    
    return ConversationHandler.END

# === Language Configuration ===
async def set_language_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start language configuration (Owner only)"""
    user_id = update.message.from_user.id
    if not is_owner(user_id):
        await update.message.reply_text("❌ " + get_text("not_allowed_error", user_id))
        return ConversationHandler.END
        
    language_options = "\n".join([f"/{lang} - {TEXTS[lang]['language_name']}" for lang in TEXTS.keys()])
    
    await update.message.reply_text(
        f"🌐 {get_text('set_language', user_id)}\n"
        f"{get_text('available_languages', user_id)}:\n"
        f"{language_options}\n"
        f"{get_text('current_language', user_id)}: {TEXTS[BOT_CONFIG['default_language']]['language_name']}"
    )
    return SET_LANGUAGE

async def set_language_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """End language configuration"""
    user_id = update.message.from_user.id
    try:
        language = update.message.text.strip().lower().replace('/', '')
        
        if language in TEXTS:
            BOT_CONFIG["default_language"] = language
            await update.message.reply_text(
                f"✅ {get_text('language_set', user_id)}\n"
                f"{get_text('new_language', user_id)}: {TEXTS[language]['language_name']}",
                reply_markup=get_admin_keyboard(user_id)
            )
        else:
            await update.message.reply_text(f"❌ {get_text('invalid_language', user_id)}")
    
    except Exception as e:
        await update.message.reply_text(f"❌ {get_text('error', user_id)}: {e}")
    
    return ConversationHandler.END

async def add_allowed_user_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start adding allowed user (Owner only)"""
    user_id = update.message.from_user.id
    if not is_owner(user_id):
        await update.message.reply_text("❌ " + get_text("not_allowed_error", user_id))
        return ConversationHandler.END
        
    await update.message.reply_text(
        f"👤 {get_text('add_allowed_user', user_id)}\n"
        f"{get_text('enter_user_id', user_id)}:"
    )
    return ADD_ALLOWED_USER

async def add_allowed_user_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """End adding allowed user"""
    user_id = update.message.from_user.id
    try:
        user_id_to_add = int(update.message.text.strip())
        
        if user_id_to_add not in BOT_CONFIG["allowed_users"]:
            BOT_CONFIG["allowed_users"].append(user_id_to_add)
            await update.message.reply_text(
                f"✅ {get_text('user_added', user_id)}\n"
                f"{get_text('user_id', user_id)}: {user_id_to_add}",
                reply_markup=get_admin_keyboard(user_id)
            )
        else:
            await update.message.reply_text(f"⚠️ {get_text('user_already_added', user_id)}")
    
    except ValueError:
        await update.message.reply_text(f"❌ {get_text('invalid_user_id', user_id)}")
    except Exception as e:
        await update.message.reply_text(f"❌ {get_text('error', user_id)}: {e}")
    
    return ConversationHandler.END

async def list_allowed_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List allowed users (Owner only)"""
    user_id = update.message.from_user.id
    if not is_owner(user_id):
        await update.message.reply_text("❌ " + get_text("not_allowed_error", user_id))
        return

    if not BOT_CONFIG["allowed_users"]:
        await update.message.reply_text(f"📝 {get_text('no_allowed_users', user_id)}")
        return

    users_info = [f"✅ {get_text('allowed_users', user_id)}:"]
    for allowed_user_id in BOT_CONFIG["allowed_users"]:
        try:
            user = await context.bot.get_chat(allowed_user_id)
            username = f"@{user.username}" if user.username else get_text("no_username", user_id)
            users_info.append(f"- {user.first_name} ({username}, ID: {allowed_user_id})")
        except Exception as e:
            users_info.append(f"- {get_text('unknown_user', user_id)} (ID: {allowed_user_id}) - {get_text('error', user_id)}: {e}")

    await update.message.reply_text("\n".join(users_info))

# === Direct Language Commands ===
async def set_language_khmer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set language to Khmer"""
    user = update.message.from_user
    user_id = user.id
    
    user_languages[user_id] = "khmer"
    await update.message.reply_text(
        f"✅ {get_text('language_updated', user_id)}\n"
        f"{get_text('your_language', user_id)}: {TEXTS['khmer']['language_name']}",
        reply_markup=get_keyboard(user_id)
    )
    logger.info(f"User {user_id} changed language to khmer")

async def set_language_chinese(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set language to Chinese"""
    user = update.message.from_user
    user_id = user.id
    
    user_languages[user_id] = "chinese"
    await update.message.reply_text(
        f"✅ {get_text('language_updated', user_id)}\n"
        f"{get_text('your_language', user_id)}: {TEXTS['chinese']['language_name']}",
        reply_markup=get_keyboard(user_id)
    )
    logger.info(f"User {user_id} changed language to chinese")

async def set_language_english(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set language to English"""
    user = update.message.from_user
    user_id = user.id
    
    user_languages[user_id] = "english"
    await update.message.reply_text(
        f"✅ {get_text('language_updated', user_id)}\n"
        f"{get_text('your_language', user_id)}: {TEXTS['english']['language_name']}",
        reply_markup=get_keyboard(user_id)
    )
    logger.info(f"User {user_id} changed language to english")

# === Updated Bot Handlers ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user = update.message.from_user
    user_id = user.id
    chat = update.message.chat
    
    # Initialize user language if not set
    if user_id not in user_languages:
        user_languages[user_id] = BOT_CONFIG["default_language"]
    
    # Store user name
    reset_if_new_day(user_id)
    user_records[user_id]["name"] = user.first_name
    
    # Update group members when user starts bot
    if chat.type in ["group", "supergroup"]:
        if await is_approved_group(chat.id):
            await update_group_members(chat.id, context.bot)
            await track_user_activity(chat.id, user)
    
    if chat.type == "private":
        if is_owner(user_id):
            await update.message.reply_text(
                f"👑 {get_text('config', user_id)}\n{get_text('status', user_id)}: {get_text('success', user_id)}",
                reply_markup=get_admin_keyboard(user_id)
            )
        else:
            welcome_msg = (
                f"{get_text('welcome', user_id)}\n"
                f"{get_text('status', user_id)}: {get_text('success', user_id)}\n"
                f"🌐 Language: {TEXTS[get_user_language(user_id)]['language_name']}\n"
                f"{get_text('language_help', user_id)}\n"
                f"Quick commands:\n"
                f"/km - ភាសាខ្មែរ\n"
                f"/ch - 中文\n" 
                f"/en - English"
            )
            await update.message.reply_text(
                welcome_msg,
                reply_markup=get_keyboard(user_id)
            )
    else:
        # Group chat
        if not await is_approved_group(chat.id):
            await update.message.reply_text("❌ " + get_text("not_allowed_error", user_id))
            return
            
        # Update group members
        await update_group_members(chat.id, context.bot)
        await track_user_activity(chat.id, user)
        
        # Send status message with keyboard
        welcome_msg = (
            f"{get_text('welcome', user_id)}\n"
            f"🌐 Language: {TEXTS[get_user_language(user_id)]['language_name']}\n"
            f"{get_text('language_help', user_id)}\n"
            f"Quick commands:\n"
            f"/km - ភាសាខ្មែរ\n"
            f"/ch - 中文\n"
            f"/en - English"
        )
        try:
            await context.bot.send_message(
                chat_id=chat.id,
                text=welcome_msg,
                reply_markup=get_keyboard(user_id)
            )
        except Exception as e:
            logger.error(f"Failed to send welcome message: {e}")

async def welcome_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send welcome message when new members join the group"""
    if update.message.new_chat_members:
        chat = update.message.chat
        
        # Check if this is an approved group
        if not await is_approved_group(chat.id):
            return
        
        # Update group members when new members join
        await update_group_members(chat.id, context.bot)
            
        # Send status message to welcome new members
        for member in update.message.new_chat_members:
            if not member.is_bot:
                await track_user_activity(chat.id, member)
                await send_status_message(
                    chat.id, 
                    context.bot, 
                    f"👋 Welcome {member.first_name} to the group!\n"
                )
                logger.info(f"Sent welcome message for {member.first_name} in group {chat.id}")

async def checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle check-in messages"""
    user = update.message.from_user
    user_id = user.id
    chat = update.message.chat
    
    if chat.type != "private" and not await is_approved_group(chat.id):
        await update.message.reply_text("❌ " + get_text("not_allowed_error", user_id))
        return

    action = update.message.text.strip()
    now = get_current_time()
    
    # Update group members on any interaction and track user activity
    if chat.type in ["group", "supergroup"]:
        await update_group_members(chat.id, context.bot)
        await track_user_activity(chat.id, user)
    
    # Ensure user record exists and is reset for new day
    reset_if_new_day(user_id)
    
    record = user_records[user_id]
    record["name"] = user.first_name

    # === Status Query ===
    if action == get_text("status", user_id):
        total_penalty = 0
        for p in record.get("penalties", []):
            if "៛" in p:
                try:
                    penalty_amount = int(p.split("៛")[0].split()[-1])
                    total_penalty += penalty_amount
                except (ValueError, IndexError):
                    continue

        current_activity = f"{get_text('working_on', user_id)}: {record['active'][0]}" if record.get("active") else get_text("none", user_id)
        
        msg = (
            f"📊 {get_text('personal_status', user_id)}\n"
            f"{get_text('user', user_id)}: {user.first_name}\n"
            f"{get_text('current_activity', user_id)}: {current_activity}\n"
            f"{get_text('work_today', user_id)}: {format_duration(record.get('work_total', 0))}\n"
            f"{get_text('penalty_today', user_id)}: {total_penalty}{get_text('currency', user_id)}\n"
            f"{get_text('meal_count', user_id)}: {record['counts'].get(get_text('meal', user_id), 0)}{get_text('times', user_id)}\n"
            f"{get_text('toilet_count', user_id)}: {record['counts'].get(get_text('toilet', user_id), 0)}{get_text('times', user_id)}"
        )
        await update.message.reply_text(msg, reply_markup=get_keyboard(user_id))
        return

    # === Start Work ===
    if action == get_text("start_work", user_id):
        if record.get("work_start") is not None:
            await update.message.reply_text(
                f"{get_text('user', user_id)}: {user.first_name}\n"
                f"{get_text('user_id', user_id)}: {user.id}\n"
                f"{get_text('status', user_id)}: ❌ {get_text('error', user_id)} {now.strftime('%m/%d %H:%M:%S')}\n"
                f"{get_text('reason', user_id)}: {get_text('already_started_work', user_id)}",
                reply_markup=get_keyboard(user_id)
            )
            return

        record["work_start"] = now
        
        # Check for late arrival
        if now.time() > BOT_CONFIG["work_start_limit"]:
            work_start_limit_time = datetime.combine(now.date(), BOT_CONFIG["work_start_limit"])
            work_start_limit_time = make_timezone_aware(work_start_limit_time)
            
            late_duration = int((now - work_start_limit_time).total_seconds())
            fine = FINES["late"]
            
            record.setdefault("penalties", []).append(f"{get_text('late_penalty', user_id)} {fine}{get_text('currency', user_id)}")
            
            msg = (
                f"{get_text('user', user_id)}: {user.first_name}\n"
                f"{get_text('user_id', user_id)}: {user.id}\n"
                f"⚠️ {get_text('warning', user_id)}: {get_text('late_arrival', user_id)}!\n"
                f"{get_text('late_duration', user_id)}: {format_duration(late_duration)}\n"
                f"{get_text('info', user_id)}: {get_text('late_recorded', user_id)}\n"
                f"{get_text('late_penalty_this_time', user_id)}: {fine}{get_text('currency', user_id)}\n"
                f"——————————————\n"
                f"✅ {get_text('success', user_id)}: {get_text('start_work', user_id)} - {now.strftime('%m/%d %H:%M:%S')}\n"
                f"{get_text('info', user_id)}: {get_text('good_morning', user_id)}!\n"
                f"{get_text('free_version', user_id)}"
            )
            await update.message.reply_text(msg, reply_markup=get_keyboard(user_id))
            await send_penalty(context, user, get_text("start_work", user_id), get_text("late", user_id), fine, late_duration)
            
        # Check for night shift
        elif now.time() >= BOT_CONFIG["work_overtime_limit"]:
            fine = FINES["night"]
            record.setdefault("penalties", []).append(f"{get_text('night_penalty', user_id)} {fine}{get_text('currency', user_id)}")
            
            msg = (
                f"{get_text('user', user_id)}: {user.first_name}\n"
                f"{get_text('user_id', user_id)}: {user.id}\n"
                f"✅ {get_text('success', user_id)}: {get_text('start_work', user_id)} - {now.strftime('%m/%d %H:%M:%S')}\n"
                f"{get_text('info', user_id)}: {get_text('good_morning', user_id)}!\n"
                f"{get_text('free_version', user_id)}"
            )
            await update.message.reply_text(msg, reply_markup=get_keyboard(user_id))
            await send_penalty(context, user, get_text("start_work", user_id), get_text("night_shift_after_10pm", user_id), fine)
            
        else:
            msg = (
                f"{get_text('user', user_id)}: {user.first_name}\n"
                f"{get_text('user_id', user_id)}: {user.id}\n"
                f"✅ {get_text('success', user_id)}: {get_text('start_work', user_id)} - {now.strftime('%m/%d %H:%M:%S')}\n"
                f"{get_text('info', user_id)}: {get_text('good_morning', user_id)}!\n"
                f"{get_text('free_version', user_id)}"
            )
            await update.message.reply_text(msg, reply_markup=get_keyboard(user_id))
        return

    # === End Work ===
    if action == get_text("end_work", user_id):
        if not record.get("work_start"):
            await update.message.reply_text("⚠️ No work start record", reply_markup=get_keyboard(user_id))
            return
            
        work_start_time = record["work_start"]
        duration = int((now - work_start_time).total_seconds())
        record["work_total"] = record.get("work_total", 0) + duration
        record["work_start"] = None
        
        # End any active activity
        if record.get("active"):
            last_action, start_time = record["active"]
            activity_duration = int((now - start_time).total_seconds())
            record["times"][last_action] = record["times"].get(last_action, 0) + activity_duration
            record["times"]["total"] = record["times"].get("total", 0) + activity_duration
            record["active"] = None

        pure_work_time = record.get("work_total", 0) - record["times"].get("total", 0)
        if pure_work_time < 0:
            pure_work_time = 0

        msg = (
            f"{get_text('user', user_id)}: {user.first_name}\n"
            f"{get_text('user_id', user_id)}: {user.id}\n"
            f"✅ {get_text('success', user_id)}: {get_text('end_work', user_id)} - {now.strftime('%m/%d %H:%M:%S')}\n"
            f"{get_text('info', user_id)}: Today's work time has been recorded\n"
            f"Total work today: {format_duration(record.get('work_total', 0))}\n"
            f"Pure work time: {format_duration(pure_work_time)}\n"
            f"------------------------\n"
            f"Total active time today: {format_duration(record['times'].get('total', 0))}\n"
            f"Total rest breaks today: {record['counts'].get(get_text('rest', user_id), 0)} {get_text('times', user_id)}\n"
            f"Total rest time today: {format_duration(record['times'].get(get_text('rest', user_id), 0))}\n"
            f"Total toilet breaks today: {record['counts'].get(get_text('toilet', user_id), 0)} {get_text('times', user_id)}\n"
            f"Total toilet time today: {format_duration(record['times'].get(get_text('toilet', user_id), 0))}\n"
            f"Total smoke breaks today: {record['counts'].get(get_text('smoke', user_id), 0)} {get_text('times', user_id)}\n"
            f"Total smoke time today: {format_duration(record['times'].get(get_text('smoke', user_id), 0))}\n"
            f"Total meal 1 today: {record['counts'].get(get_text('meal_count_1', user_id), 0)} {get_text('times', user_id)}\n"
            f"Total meal 1 time today: {format_duration(record['times'].get(get_text('meal_count_1', user_id), 0))}\n"
            f"Total meal 2 today: {record['counts'].get(get_text('meal_count_2', user_id), 0)} {get_text('times', user_id)}\n"
            f"Total meal 2 time today: {format_duration(record['times'].get(get_text('meal_count_2', user_id), 0))}\n"
            f"{get_text('free_version', user_id)}"
        )
        await update.message.reply_text(msg, reply_markup=get_keyboard(user_id))
        return

    # === Activity Start (Meal/Toilet/Smoke) ===
    if action in [get_text("meal", user_id), get_text("toilet", user_id), get_text("smoke", user_id)]:
        if record.get("active"):
            await update.message.reply_text(
                f"⚠️ You are currently {record['active'][0]}, please return first",
                reply_markup=get_keyboard(user_id)
            )
            return
        
        user_warnings[user_id] = False
            
        if action == get_text("meal", user_id):
            meal_count = record['counts'].get(get_text("meal_count_1", user_id), 0) + record['counts'].get(get_text("meal_count_2", user_id), 0)
            if meal_count == 0:
                action_type = get_text("meal_count_1", user_id)
            else:
                action_type = get_text("meal_count_2", user_id)
            record["counts"][action_type] = record["counts"].get(action_type, 0) + 1
            record["active"] = (action_type, now)
            time_limit = ACTIVITY_LIMITS["meal"]
        else:
            record["counts"][action] = record["counts"].get(action, 0) + 1
            record["active"] = (action, now)
            time_limit = ACTIVITY_LIMITS[action]
            action_type = action

        return_time = (now + timedelta(minutes=time_limit)).strftime('%H:%M')
        count = record["counts"].get(action_type, 0)
        
        msg = (
            f"{get_text('user', user_id)}: {user.first_name}\n"
            f"{get_text('user_id', user_id)}: {user.id}\n"
            f"✅ {get_text('success', user_id)}: {action_type} - {now.strftime('%m/%d %H:%M:%S')}\n"
            f"Note: This is your {count} time {action_type}\n"
            f"Activity time limit: {time_limit} minutes\n"
            f"Please return before: {return_time}\n"
            f"{get_text('info', user_id)}: Please check-in return immediately after finishing activity\n"
            f"Return: /back\n"
            f"{get_text('free_version', user_id)}"
        )
        await update.message.reply_text(msg, reply_markup=get_keyboard(user_id))
        return

    # === Return to Seat ===
    if action == get_text("back", user_id):
        if not record.get("active"):
            await update.message.reply_text("⚠️ You haven't started any activity", reply_markup=get_keyboard(user_id))
            return
            
        last_action, start_time = record["active"]
        duration = int((now - start_time).total_seconds())

        record["times"][last_action] = record["times"].get(last_action, 0) + duration
        record["times"]["total"] = record["times"].get("total", 0) + duration
        record["active"] = None
        
        user_warnings[user_id] = False

        overtime_msg = ""
        if last_action in [get_text("meal_count_1", user_id), get_text("meal_count_2", user_id)]:
            base_action = get_text("meal", user_id)
        else:
            base_action = last_action
            
        if base_action in ACTIVITY_LIMITS and duration > ACTIVITY_LIMITS[base_action] * 60:
            fine = FINES["overtime"]
            record.setdefault("penalties", []).append(f"{last_action} overtime penalty {fine}{get_text('currency', user_id)}")
            overtime_msg = f"\n⚠️ Note: {last_action} overtime, penalty{fine}{get_text('currency', user_id)}"
            await send_penalty(context, user, last_action, "Activity overtime", fine, duration)

        activity_summary = f"{get_text('rest', user_id)} today: {record['counts'].get(get_text('rest', user_id), 0)} {get_text('times', user_id)}\n{get_text('toilet', user_id)} today: {record['counts'].get(get_text('toilet', user_id), 0)} {get_text('times', user_id)}\n"

        msg = (
            f"{get_text('user', user_id)}: {user.first_name}\n"
            f"{get_text('user_id', user_id)}: {user.id}\n"
            f"✅ {now.strftime('%m/%d %H:%M:%S')} {get_text('return_from_break', user_id)}: {last_action}\n"
            f"{get_text('info', user_id)}: This activity time has been recorded\n"
            f"{get_text('activity_time_used', user_id)}: {format_duration(duration)}\n"
            f"{get_text('total_activity_time', user_id)}: {format_duration(record['times'].get(last_action, 0))}\n"
            f"{get_text('total_active_time', user_id)}: {format_duration(record['times'].get('total', 0))}\n"
            f"------------------------\n"
            f"{activity_summary}"
            f"{overtime_msg}\n"
            f"{get_text('free_version', user_id)}"
        )
        await update.message.reply_text(msg, reply_markup=get_keyboard(user_id))
        return

    # Unknown command
    await update.message.reply_text("Unknown command, please use the reply keyboard", reply_markup=get_keyboard(user_id))

async def handle_new_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle bot added to new group with user verification"""
    if update.message.new_chat_members:
        for member in update.message.new_chat_members:
            if member.id == context.bot.id:
                chat = update.message.chat
                user = update.message.from_user
                
                if chat.type in ["group", "supergroup"]:
                    # Check if user has username (if required)
                    if BOT_CONFIG["require_username"] and not has_username(user):
                        try:
                            await update.message.reply_text(
                                f"❌ {get_text('no_username_error', user.id)}"
                            )
                            await asyncio.sleep(2)
                            await context.bot.leave_chat(chat.id)
                            logger.info(f"Left group {chat.title} - user {user.id} has no username")
                            return
                        except Exception as e:
                            logger.error(f"Error handling no username case: {e}")
                    
                    # Check if user is allowed to add bot
                    if is_allowed_user(user.id) or is_owner(user.id):
                        # User is allowed - auto approve
                        approved_groups.add(chat.id)
                        await update_group_members(chat.id, context.bot)
                        
                        try:
                            await send_status_message(chat.id, context.bot, f"✅ {get_text('group_approved', user.id)}\n")
                        except Exception as e:
                            logger.warning(f"Could not send welcome message to {chat.id}: {e}")
                            
                        logger.info(f"Auto-approved group {chat.title} (ID: {chat.id}) added by user {user.id}")
                    else:
                        # Unauthorized user added bot to group
                        user_info = await get_user_detailed_info(user)
                        
                        attempt_info = {
                            'timestamp': get_current_time().strftime("%Y-%m-%d %H:%M:%S"),
                            'user_id': user.id,
                            'user_name': user.first_name,
                            'username': f"@{user.username}" if user.username else get_text("no_username", user.id),
                            'language_code': user.language_code if user.language_code else get_text("unknown", user.id),
                            'is_premium': getattr(user, 'is_premium', False),
                            'group_id': chat.id,
                            'group_name': chat.title,
                            'full_user_info': user_info
                        }
                        unauthorized_attempts.append(attempt_info)
                        
                        # Send notification to owner
                        detailed_msg = (
                            f"🚨 {get_text('unauthorized_group_addition', None)}\n"
                            f"{get_text('user', None)}: {user.first_name} (ID: {user.id})\n"
                            f"{get_text('group', None)}: {chat.title} (ID: {chat.id})\n"
                            f"{get_text('time', None)}: {get_current_time().strftime('%Y-%m-%d %H:%M:%S')}\n"
                            f"{get_text('bot_will_leave', None)}"
                        )
                        
                        try:
                            await context.bot.send_message(chat_id=OWNER_ID, text=detailed_msg)
                        except Exception as e:
                            logger.error(f"Failed to send notification to owner: {e}")
                        
                        # Send messages and leave group
                        try:
                            # First message
                            await update.message.reply_text(
                                f"{get_text('unauthorized_detected', user.id)}\n"
                                f"{get_text('unauthorized_processing', user.id)}\n"
                            )
                            
                            await asyncio.sleep(1)

                            # Second message
                            await update.message.reply_text(
                                f"{get_text('unauthorized_leaving', user.id)}\n"
                                f"{get_text('unauthorized_reason', user.id)}\n"
                                f"{get_text('unauthorized_contact', user.id)}\n"
                            )
                            
                            await asyncio.sleep(2)
                            
                        except Exception as e:
                            logger.warning(f"Could not send messages to unauthorized group {chat.id}: {e}")
                        
                        # Leave the group
                        try:
                            await context.bot.leave_chat(chat.id)
                            logger.info(f"✅ Successfully left unauthorized group: {chat.title} (ID: {chat.id}) added by user {user.id}")
                        except Exception as e:
                            logger.error(f"❌ Failed to leave group {chat.id}: {e}")
                            try:
                                await context.bot.send_message(
                                    chat_id=OWNER_ID,
                                    text=f"⚠️ CRITICAL: {get_text('failed_to_leave_group', None)} {chat.title} (ID: {chat.id}). {get_text('please_remove_manually', None)}"
                                )
                            except:
                                pass

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel conversation"""
    user_id = update.message.from_user.id
    await update.message.reply_text("Operation cancelled", reply_markup=get_keyboard(user_id))
    return ConversationHandler.END

def main_with_restart():
    """Main function with auto-restart for Render compatibility"""
    # Start Flask server in a separate thread for health checks
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("✅ Flask health check server started on port 5000")
    
    # Start keep-alive service to prevent Render sleep
    keep_alive()
    logger.info("✅ Keep-alive service started")
    
    while True:
        try:
            logger.info("🤖 Starting Telegram Bot...")
            
            # Create application
            app = Application.builder().token(TOKEN).build()
            
            # Schedule jobs with configurable times
            app.job_queue.run_daily(
                reset_all_records, 
                time=BOT_CONFIG["daily_reset_time"]
            )
            app.job_queue.run_daily(
                broadcast_penalties, 
                time=BOT_CONFIG["summary_time"]
            )
            app.job_queue.run_repeating(check_activity_warnings, interval=30, first=10)
            app.job_queue.run_repeating(check_activity_overtime, interval=300, first=10)
            
            # Add periodic group member updates
            async def update_all_group_members(context: ContextTypes.DEFAULT_TYPE):
                """Update members for all approved groups"""
                for group_id in approved_groups:
                    await update_group_members(group_id, context.bot)
            
            app.job_queue.run_repeating(update_all_group_members, interval=3600, first=10)
            
            # Send startup message when bot starts
            app.job_queue.run_once(send_startup_message, when=5)
            
            # Conversation handlers
            group_conv = ConversationHandler(
                entry_points=[CommandHandler("approve_group", group_approval_start)],
                states={
                    APPROVE_GROUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, group_approval_end)]
                },
                fallbacks=[CommandHandler("cancel", cancel)]
            )
            
            work_time_conv = ConversationHandler(
                entry_points=[CommandHandler("set_work_time", set_work_time_start)],
                states={
                    SET_WORK_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_work_time_end)]
                },
                fallbacks=[CommandHandler("cancel", cancel)]
            )
            
            reset_time_conv = ConversationHandler(
                entry_points=[CommandHandler("set_reset_time", set_reset_time_start)],
                states={
                    SET_RESET_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_reset_time_end)]
                },
                fallbacks=[CommandHandler("cancel", cancel)]
            )
            
            language_conv = ConversationHandler(
                entry_points=[CommandHandler("set_language", set_language_start)],
                states={
                    SET_LANGUAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_language_end)]
                },
                fallbacks=[CommandHandler("cancel", cancel)]
            )
            
            allowed_user_conv = ConversationHandler(
                entry_points=[CommandHandler("add_allowed_user", add_allowed_user_start)],
                states={
                    ADD_ALLOWED_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_allowed_user_end)]
                },
                fallbacks=[CommandHandler("cancel", cancel)]
            )
            
            # Add handlers
            app.add_handler(CommandHandler("start", start))
            app.add_handler(CommandHandler("stats", admin_stats))
            app.add_handler(CommandHandler("export", export_data))
            app.add_handler(CommandHandler("list_groups", list_groups))
            app.add_handler(CommandHandler("unauthorized_logs", unauthorized_logs))
            app.add_handler(CommandHandler("broadcast", broadcast))
            app.add_handler(CommandHandler("show_config", show_config))
            app.add_handler(CommandHandler("list_allowed_users", list_allowed_users))
            
            # Add direct language command handlers
            app.add_handler(CommandHandler("km", set_language_khmer))
            app.add_handler(CommandHandler("ch", set_language_chinese))
            app.add_handler(CommandHandler("en", set_language_english))
            
            app.add_handler(group_conv)
            app.add_handler(work_time_conv)
            app.add_handler(reset_time_conv)
            app.add_handler(language_conv)
            app.add_handler(allowed_user_conv)
            app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, checkin))
            app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, welcome_new_member))
            app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, handle_new_group))
            
            # Initialize approved groups with the main group
            approved_groups.add(GROUP_ID)
            
            logger.info(f"✅ Bot configured with per-user language support. Default language: {BOT_CONFIG['default_language']}")
            logger.info("🚀 Starting bot polling...")
            
            # Run the bot with polling
            app.run_polling(
                drop_pending_updates=True,
                allowed_updates=Update.ALL_TYPES,
                close_loop=False
            )
            
        except KeyboardInterrupt:
            logger.info("🛑 Bot stopped by user")
            break
        except Exception as e:
            logger.error(f"❌ Bot crashed: {e}")
            logger.info("🔄 Restarting in 30 seconds...")
            time.sleep(30)
            continue

if __name__ == "__main__":
    logger.info("🎯 Starting Telegram Shift Bot with Render.com compatibility")
    main_with_restart()