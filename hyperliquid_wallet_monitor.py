import asyncio
import aiohttp
import json
import sys
import re

# ==============================================================================
# ⚙️ CONFIGURATION & CREDENTIALS
# ==============================================================================
TELEGRAM_BOT_TOKEN = "8977965771:AAGZRRCyMak4s7LKYSDMYsZQG1a2Nfibc4U"
TELEGRAM_CHAT_ID = "451514570"
HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"

# Track an observed account or leave global
TARGET_ADDRESS = "0x833b99b27dac651d02080f5e220e929df891db06" 

# ==============================================================================
def md_escape(text: str) -> str:
    """Escapes special markdown formatting characters required by Telegram."""
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in escape_chars else c for c in str(text))

async def send_filtered_alert(session: aiohttp.ClientSession, log_line: str, ticker: str):
    """Sends a beautifully formatted Markdown alert directly to your Telegram bot."""
    url = f"https://telegram.org{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    msg = (
        f"🚨 *Watchlist Confluence Target*\n\n"
        f"🪙 *Asset:* `{md_escape(ticker)}`\n"
        f"🎯 *Status:* `HIGH-PROBABILITY CRITERIA MET`\n\n"
        f"📊 *Scan Details:* \n_{md_escape(log_line)}_"
    )
    
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "MarkdownV2"
    }
    
    try:
        async with session.post(url, json=payload, timeout=10) as response:
            if response.status != 200:
                resp_text = await response.text()
                sys.stderr.write(f"Telegram API Error: {response.status} - {resp_text}\n")
    except Exception as e:
        sys.stderr.write(f"Network Error sending to Telegram: {str(e)}\n")

async def keep_alive_ping(ws: aiohttp.ClientWebSocketResponse):
    """Sends a mandatory heartbeat frame every 30 seconds to prevent connection drops."""
    try:
        while True:
            await asyncio.sleep(30)
            if not ws.closed:
                await ws.send_json({"method": "ping"})
                sys.stdout.write("[INFO] Sent WebSocket Heartbeat (ping)\n")
                sys.stdout.flush()
    except asyncio.CancelledError:
        pass

async def handle_websocket_stream():
    score_pattern = re.compile(r"Score\s*[:=]\s*([0-9.]+)", re.IGNORECASE)
    proximity_pattern = re.compile(r"within\s*([0-9.]+)%", re.IGNORECASE)

    async with aiohttp.ClientSession() as session:
        sys.stdout.write("[INFO] Connecting to Hyperliquid WebSocket Pipeline...\n")
        sys.stdout.flush()
        
        async with session.ws_connect(HYPERLIQUID_WS_URL) as ws:
            sys.stdout.write("[INFO] WebSocket Pipeline Connected Successfully!\n")
            
            # Subscribe to the target channel (webData2 for an active address profile)
            subscribe_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "webData2",
                    "user": TARGET_ADDRESS
                }
            }
            await ws.send_json(subscribe_msg)
            sys.stdout.write(f"[INFO] Streaming data channel active for address: {TARGET_ADDRESS}\n")
            sys.stdout.flush()
            
            # Spawn the concurrent background task keeping the connection open
            ping_task = asyncio.create_task(keep_alive_ping(ws))
            
            try:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        
                        # Process connection acknowledgements/pong updates
                        if data.get("channel") == "pong" or "pong" in str(data):
                            continue
                            
                        # Standardize structured socket payload string data to mimic logic logs
                        data_str = json.dumps(data)
                        line_upper = data_str.upper()
                        
                        # --- CRITERIA FILTER CHECKS ---
                        if "HIGH-PROBABILITY" not in line_upper and "SETUP" not in line_upper:
                            continue
                        if "FIB" not in line_upper and "FIBONACCI" not in line_upper:
                            continue
                        if "SMA" not in line_upper and "50" not in line_upper and "200" not in line_upper:
                            continue
                            
                        score_match = score_pattern.search(data_str)
                        prox_match = proximity_pattern.search(data_str)
                        
                        if score_match and prox_match:
                            score_val = float(score_match.group(1))
                            prox_val = float(prox_match.group(1))
                            
                            if score_val >= 7.0 and prox_val <= 2.0:
                                # Fallback lookup extraction for the raw text ticker asset string name
                                ticker_match = re.search(r'([A-Z0-9]{2,10})', data_str)
                                ticker = ticker_match.group(1) if ticker_match else "Asset Detected"
                                
                                await send_filtered_alert(session, data_str, ticker)
                                
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            finally:
                ping_task.cancel()

async def main():
    # Production auto-healing reconnection mechanism loop
    while True:
        try:
            await handle_websocket_stream()
        except Exception as e:
            sys.stderr.write(f"[ERROR] WebSocket disconnected: {str(e)}. Reconnecting in 5s...\n")
            sys.stderr.flush()
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
