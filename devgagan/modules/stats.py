# ---------------------------------------------------
# File Name: stats.py
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



import time
import sys
import motor
from devgagan import app
from pyrogram import filters
from config import OWNER_ID
from devgagan.core.mongo.users_db import get_users, add_user, get_user
from devgagan.core.mongo.plans_db import premium_users
from pyrogram.types import InputFile
import os

start_time = time.time()

@app.on_message(group=10)
async def chat_watcher_func(_, message):
    try:
        if message.from_user:
            us_in_db = await get_user(message.from_user.id)
            if not us_in_db:
                await add_user(message.from_user.id)
    except:
        pass

def time_formatter():
    minutes, seconds = divmod(int(time.time() - start_time), 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    weeks, days = divmod(days, 7)
    tmp = (
        ((str(weeks) + "w:") if weeks else "")
        + ((str(days) + "d:") if days else "")
        + ((str(hours) + "h:") if hours else "")
        + ((str(minutes) + "m:") if minutes else "")
        + ((str(seconds) + "s") if seconds else "")
    )
    if tmp != "":
        if tmp.endswith(":"):
            return tmp[:-1]
        else:
            return tmp
    else:
        return "0 s"

@app.on_message(filters.command("stats"))
async def stats(client, message):
    start = time.time()
    users = await get_users()
    premium = await premium_users()
    ping = round((time.time() - start) * 1000)
    
    if message.from_user.id in OWNER_ID:
        # Create a file with user IDs and usernames
        file_path = "user_stats.txt"
        with open(file_path, "w") as file:
            file.write("User IDs and Usernames:\n")
            for user in users:
                user_info = await client.get_users(user['_id'])
                file.write(f"{user['_id']} - {user_info.first_name}\n")

        # Send the file to the owner
        sent_message = await client.send_document(
            chat_id=OWNER_ID[0],
            document=InputFile(file_path),
            caption="Here are the user IDs and usernames."
        )
        
        # Get the link of the sent file
        file_link = sent_message.link

        # Send the stats message with the file link
        await message.reply_text(f"""
**Stats of** {(await client.get_me()).mention} :

🏓 **Ping Pong**: {ping}ms

📊 **Total Users** : `{len(users)}`
📈 **Premium Users** : `{len(premium)}`
⚙️ **Bot Uptime** : `{time_formatter()}`
    
📄 **User Details File**: [Download here]({file_link})
    
🎨 **Python Version**: `{sys.version.split()[0]}`
📑 **Mongo Version**: `{motor.version}`
""")
        
        # Remove the file after sending
        os.remove(file_path)
    else:
        await message.reply_text(f"""
**Stats of** {(await client.get_me()).mention} :

🏓 **Ping Pong**: {ping}ms

📊 **Total Users** : `{len(users)}`
📈 **Premium Users** : `{len(premium)}`
⚙️ **Bot Uptime** : `{time_formatter()}`
    
🎨 **Python Version**: `{sys.version.split()[0]}`
📑 **Mongo Version**: `{motor.version}`
""")
