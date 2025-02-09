# devgagan
# Note if you are trying to deploy on vps then directly fill values in ("")

from os import getenv

# VPS --- FILL COOKIES 🍪 in """ ... """ 

INST_COOKIES = """
# wtite up here insta cookies
"""

YTUB_COOKIES = """
# write here yt cookies
"""

API_ID = int(getenv("API_ID", "28777385"))
API_HASH = getenv("API_HASH", "acf3ad8683c4475aa0e33910904eab82")
BOT_TOKEN = getenv("BOT_TOKEN", "")
OWNER_ID = list(map(int, getenv("OWNER_ID", "922270982").split()))
MONGO_DB = getenv("MONGO_DB", "")
LOG_GROUP = getenv("LOG_GROUP", "-1002362885377")
CHANNEL_ID = int(getenv("CHANNEL_ID", "-1002437305944"))
FREEMIUM_LIMIT = int(getenv("FREEMIUM_LIMIT", "50"))
PREMIUM_LIMIT = int(getenv("PREMIUM_LIMIT", "5000000000000"))
WEBSITE_URL = getenv("WEBSITE_URL", None)
AD_API = getenv("AD_API", None)
STRING = getenv("STRING", None)
YT_COOKIES = getenv("YT_COOKIES", None)
INSTA_COOKIES = getenv("INSTA_COOKIES", None)
