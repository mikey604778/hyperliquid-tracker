import asyncio
import json
import logging
import sys
import aiohttp
import websockets

# Setup basic logging to force terminal printouts immediately
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("HLMonitor")

# ==============================================================================
# ⚙️ CONFIGURATION - ACTIVE SYSTEM RUNTIME CREDENTIALS
# ==============================================================================
TELEGRAM_BOT_TOKEN = "8977965771:AAGZRRCyMak4s7LKYSDMYsZQG1a2Nfibc4U"
TELEGRAM_CHAT_ID = "451514570"
TARGET_WALLET = "0x833b99b27dac651d02080f5e220e929df891db06"
# ==============================================================================

HL_WS_URL = "wss://api.hyperliquid.xyz/ws"

def md_escape(text: str) -> str:
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in escape_chars else c for c in str(text))

async def send_telegram_alert(session: aiohttp.ClientSession, message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "MarkdownV2"
    }
    try:
        async with session.post(url, json=payload) as response:
            if response.status != 200:
                logger.error(f"Telegram alert failed: {await response.text()}")
    except Exception as e:
        logger.error(f"Telegram error: {e}")

async def monitor():
    wallet_clean = TARGET_WALLET.strip().lower()
    logger.info(f"Target Wallet loaded: {wallet_clean}")
    logger.info(f"Connecting to Hyperliquid L1 Core WebSocket...")
    
    async with aiohttp.ClientSession() as session:
        # Send a quick startup text directly to your Telegram chat to confirm connection works
        await send_telegram_alert(session, "🟢 *Hyperliquid Monitor Client Successfully Started*")
        
        while True:
            try:
                async with websockets.connect(HL_WS_URL, ping_interval=30, ping_timeout=10) as ws:
                    logger.info("WebSocket Pipeline Connected Successfully!")
                    
                    subscribe_msg = {
                        "method": "subscribe",
                        "subscription": {"type": "webData2", "user": wallet_clean}
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info(f"Streaming data channel active for address: {wallet_clean}")
                    
                    is_primed = False
                    async for message in ws:
                        data = json.loads(message)
                        if data.get("channel") != "webData2" or "data" not in data:
                            continue
                            
                        if not is_primed:
                            is_primed = True
                            logger.info("Initial wallet snapshot cached and discarded. Watching for live trades...")
                            continue
                            
                        # Process real-time changes
                        user_data = data.get("data", {})
                        fills = user_data.get("fills", [])
                        for fill in fills:
                            coin = fill.get("coin", "Unknown")
                            px = fill.get("px", "0")
                            sz = fill.get("sz", "0")
                            side = "🟢 BUY" if fill.get("side", "").upper() == "B" else "🔴 SELL"
                            
                            msg = (
                                f"⚡ *New Hyperliquid Trade Alert*\n\n"
                                f"👤 *Wallet:* `{md_escape(wallet_clean[:8])}...`\n"
                                f"🎬 *Action:* {side}\n"
                                f"🪙 *Asset:* {md_escape(coin)}\n"
                                f"📊 *Size:* {md_escape(sz)}\n"
                                f"💰 *Price:* ${md_escape(px)}\n"
                            )
                            await send_telegram_alert(session, msg)
            except Exception as e:
                logger.error(f"Connection dropped: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

if __name__ == "__main__":
    logger.info("Initializing Monitor System Client...")
    asyncio.run(monitor())
