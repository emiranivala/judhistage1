# ---------------------------------------------------
# File Name: gcast.py
# Description: A Pyrogram bot for downloading files from Telegram channels or groups 
#              and uploading them back to Telegram.
# Author: Gagan
# GitHub: https://github.com/devgaganin/
# Telegram: https://t.me/team_spy_pro
# YouTube: https://youtube.com/@dev_gagan
# Created: 2025-01-11
# Last Modified: 2025-01-11
# Version: 2.0.5
# License: MIT License
# ---------------------------------------------------

import asyncio
from pyrogram import filters
from pyrogram.errors import FloodWait, InputUserDeactivated, UserIsBlocked, PeerIdInvalid
from config import OWNER_ID
from devgagan import app
from devgagan.core.mongo.users_db import get_users
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta
import re
import traceback

# Initialize the scheduler
scheduler = AsyncIOScheduler()
scheduler.start()

async def send_msg(user_id, message):
    try:
        x = await message.copy(chat_id=user_id)
        try:
            await x.pin()
        except Exception:
            await x.pin(both_sides=True)
        return x
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return await send_msg(user_id, message)  # Proper await to prevent recursion issues
    except InputUserDeactivated:
        return 400, f"{user_id} : deactivated\n"
    except UserIsBlocked:
        return 400, f"{user_id} : blocked the bot\n"
    except PeerIdInvalid:
        return 400, f"{user_id} : user id invalid\n"
    except Exception:
        return 500, f"{user_id} : {traceback.format_exc()}\n"

def parse_time_duration(command):
    match = re.search(r'(\d+)(min|hour|hrs|days)', command)
    if not match:
        return None
    value, unit = match.groups()
    value = int(value)
    if unit == 'min':
        return timedelta(minutes=value)
    elif unit in ['hour', 'hrs']:
        return timedelta(hours=value)
    elif unit == 'days':
        return timedelta(days=value)
    return None

async def schedule_delete(message, delay):
    await asyncio.sleep(delay.total_seconds())
    try:
        await message.delete()
    except Exception:
        pass  # Avoid errors if the message is already deleted

@app.on_message(filters.command("gcast") & filters.user(OWNER_ID))
async def broadcast(_, message):
    if not message.reply_to_message:
        await message.reply_text("ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴍᴇssᴀɢᴇ ᴛᴏ ʙʀᴏᴀᴅᴄᴀsᴛ ɪᴛ.")
        return
    
    command = message.text
    delay = parse_time_duration(command)
    if not delay:
        await message.reply_text("Invalid time format. Use /gcast <duration> (e.g., 5min, 1hour, 24hrs).")
        return
    
    exmsg = await message.reply_text("sᴛᴀʀᴛᴇᴅ ʙʀᴏᴀᴅᴄᴀsᴛɪɴɢ!")
    all_users = await get_users() or []
    done_users = 0
    failed_users = 0
    
    for user in all_users:
        try:
            sent_message = await send_msg(user, message.reply_to_message)
            if isinstance(sent_message, type(message.reply_to_message)):
                scheduler.add_job(schedule_delete, 'date', run_date=datetime.now() + delay, args=[sent_message])
            done_users += 1
            await asyncio.sleep(0.1)
        except Exception:
            failed_users += 1
    
    await exmsg.edit_text(
        f"**Broadcasting Completed ✅**\n\n**Sent to:** `{done_users}` users\n"
        f"**Failed:** `{failed_users}` users"
    )

@app.on_message(filters.command("acast") & filters.user(OWNER_ID))
async def announced(_, message):
    if not message.reply_to_message:
        return await message.reply_text("Reply to a message to broadcast.")
    
    to_send = message.reply_to_message.id
    users = await get_users() or []
    done_users = 0
    failed_users = 0
    delay = parse_time_duration(message.text)
    
    exmsg = await message.reply_text("sᴛᴀʀᴛɪɴɢ ᴀɴɴᴏᴜɴᴄᴇᴍᴇɴᴛ...")
    
    for user in users:
        try:
            sent_message = await _.forward_messages(
                chat_id=int(user),
                from_chat_id=message.chat.id,
                message_ids=to_send
            )
            if isinstance(sent_message, type(message.reply_to_message)):
                scheduler.add_job(schedule_delete, 'date', run_date=datetime.now() + delay, args=[sent_message])
            done_users += 1
            await asyncio.sleep(1)
        except Exception:
            failed_users += 1
    
    await exmsg.edit_text(
        f"**Announcement Completed ✅**\n\n**Sent to:** `{done_users}` users\n"
        f"**Failed:** `{failed_users}` users"
    )
