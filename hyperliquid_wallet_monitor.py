import asyncio
import aiohttp
import json
import os
import sys

# ==============================================================================
# ⚙️ CONFIGURATION & CREDENTIALS
# ==============================================================================
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"

# Track an observed account, configured via Railway environment variable
TARGET_ADDRESS = os.environ["HYPERLIQUID_WALLET_ADDRESS"]

# ==============================================================================
def md_escape(text: str) -> str:
    """Escapes special markdown formatting characters required by Telegram."""
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in escape_chars else c for c in str(text))

async def send_telegram_message(session: aiohttp.ClientSession, msg: str):
    """Sends a pre-formatted MarkdownV2 message directly to your Telegram bot."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

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
                sys.stderr.flush()
    except Exception as e:
        sys.stderr.write(f"Network Error sending to Telegram: {str(e)}\n")
        sys.stderr.flush()


def _short_addr(addr: str) -> str:
    return f"{addr[:6]}...{addr[-4:]}" if len(addr) > 10 else addr


def _fmt_usd(value: float) -> str:
    value = abs(value)
    if value >= 1_000_000:
        return f"${value / 1_000_000:,.2f}M"
    if value >= 1000:
        return f"${value / 1000:,.2f}K"
    return f"${value:,.2f}"


def _sign(x: float) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def _fmt_num(value: float, decimals: int = 5) -> str:
    """Thousands-separated number, trimming trailing zeros/decimal point. Replaces the old
    `f'{value:,.0f}'` calls, which silently rounded any sub-$1 price (e.g. 0.21167) down to
    the string "0" -- the "@ 0" / "Avg Entry: 0" display bug."""
    s = f"{value:,.{decimals}f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s if s not in ("", "-") else "0"


def update_entry_price_cache(web_data2: dict, entry_px_by_coin: dict) -> None:
    """Refreshes coin -> avg entry price from a webData2 snapshot, in place."""
    for entry in web_data2.get("clearinghouseState", {}).get("assetPositions", []):
        pos = entry.get("position", {})
        coin = pos.get("coin")
        entry_px = pos.get("entryPx")
        if coin and entry_px is not None:
            entry_px_by_coin[coin] = float(entry_px)
        elif coin and coin in entry_px_by_coin:
            # Position fully closed in this snapshot - no entry price anymore.
            del entry_px_by_coin[coin]


def build_fill_alert(fill: dict, entry_px_by_coin: dict) -> str:
    """Formats a single userFills entry exactly like the reference tracker bot.

    Hyperliquid fill fields used: coin, px (fill price), sz (fill size),
    side ("B"=buy/"A"=sell), dir (human label e.g. "Open Long", "Close Short"),
    startPosition (signed position size *before* this fill), closedPnl (realized PNL
    on this fill -- 0/absent on opens and same-direction adds).

    Avg entry price comes from the side-by-side webData2 snapshot cache since
    individual fills don't carry the account's running average entry price.
    """
    coin = fill.get("coin", "?")
    px = float(fill.get("px", 0))
    sz = float(fill.get("sz", 0))
    side = fill.get("side", "B")
    start_position = float(fill.get("startPosition", 0) or 0)
    closed_pnl = float(fill.get("closedPnl", 0) or 0)

    signed_sz = sz if side == "B" else -sz
    new_position = start_position + signed_sz

    side_word = "LONG" if new_position >= 0 else "SHORT"
    icon = "🟢" if side_word == "LONG" else "🔴"

    # Action dynamics: a brand-new position (no prior exposure) is "Open"; growing an
    # existing position in the same direction is "Added". Anything else is a reduce/close,
    # handled separately below (it needs the pre-fill side, PNL, and a partial/full split).
    start_sign = _sign(start_position)
    fill_sign = _sign(signed_sz)
    is_new_position = start_sign == 0
    is_same_direction_increase = (
        not is_new_position
        and start_sign == fill_sign
        and abs(new_position) > abs(start_position)
    )

    # abs() here (and on remaining_size below) so a short's negative signed size never
    # leaks a "-" into the notional-value currency formatting.
    remaining_size = abs(new_position)
    notional = abs(remaining_size * px)
    avg_entry = entry_px_by_coin.get(coin, px)

    line1 = f"whale 1 \\({md_escape(_short_addr(TARGET_ADDRESS))}\\)"

    if is_new_position or is_same_direction_increase:
        action_text = f"Open {side_word.capitalize()}" if is_new_position else f"Added {side_word.capitalize()}"
        line2 = (
            f"{md_escape(action_text)} {md_escape(coin)} {icon} by "
            f"{md_escape(_fmt_num(sz))} @ {md_escape(_fmt_num(px))}"
        )
        line3 = (
            f"Total Size: {md_escape(_fmt_num(remaining_size))} "
            f"\\({md_escape(_fmt_usd(notional))}\\) \\| "
            f"Avg Entry: {md_escape(_fmt_num(avg_entry))}"
        )
        return f"{line1}\n{line2}\n{line3}"

    # Reduce or full close. The side being reduced is the position's side *before* this
    # fill (start_position's sign) -- new_position's sign/word is meaningless on a full
    # close since new_position is 0 there.
    reduce_side_word = "LONG" if start_sign >= 0 else "SHORT"
    is_full_close = remaining_size < 1e-9
    action_text = f"{'Closed' if is_full_close else 'Reduced'} {coin} {reduce_side_word}"

    pnl_icon = "✅" if closed_pnl >= 0 else "❌"
    pnl_sign = "+" if closed_pnl >= 0 else "-"

    line2 = f"{md_escape(action_text)} by {md_escape(_fmt_num(sz))} @ ${md_escape(_fmt_num(px))}"
    line3 = f"Closed PNL: {pnl_sign}${md_escape(_fmt_num(abs(closed_pnl)))} {pnl_icon}"
    line4 = (
        f"Remaining Size: {md_escape(_fmt_num(remaining_size))} "
        f"\\({md_escape(_fmt_usd(notional))}\\) \\| "
        f"Avg Entry: ${md_escape(_fmt_num(avg_entry))}"
    )
    return f"{line1}\n{line2}\n{line3}\n{line4}"

async def keep_alive_ping(ws: aiohttp.ClientWebSocketResponse):
    """Sends a mandatory heartbeat frame every 25 seconds to prevent connection drops."""
    try:
        while True:
            await asyncio.sleep(25)  # Drop to 25 seconds to beat the 60s timeout safely
            if not ws.closed:
                # Hyperliquid tracks liveness via this application-level JSON ping,
                # not raw WS protocol ping frames.
                await ws.send_str('{"method": "ping"}')

                sys.stdout.write("[INFO] Sent WebSocket Heartbeat (ping)\n")
                sys.stdout.flush()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        sys.stderr.write(f"[ERROR] Ping loop exception encountered: {str(e)}\n")
        sys.stderr.flush()

async def handle_websocket_stream():
    entry_px_by_coin: dict = {}

    async with aiohttp.ClientSession() as session:
        sys.stdout.write("[INFO] Connecting to Hyperliquid WebSocket Pipeline...\n")
        sys.stdout.flush()

        async with session.ws_connect(HYPERLIQUID_WS_URL) as ws:
            sys.stdout.write("[INFO] WebSocket Pipeline Connected Successfully!\n")
            sys.stdout.flush()

            # userFills gives instant per-fill alerts; webData2 runs alongside
            # purely to keep an up-to-date avg entry price per coin, since
            # individual fills don't carry the account's running avg entry.
            await ws.send_json({
                "method": "subscribe",
                "subscription": {"type": "userFills", "user": TARGET_ADDRESS}
            })
            await ws.send_json({
                "method": "subscribe",
                "subscription": {"type": "webData2", "user": TARGET_ADDRESS}
            })
            sys.stdout.write(f"[INFO] Streaming userFills + webData2 for address: {TARGET_ADDRESS}\n")
            sys.stdout.flush()

            # Spawn the concurrent background task keeping the connection open
            ping_task = asyncio.create_task(keep_alive_ping(ws))

            try:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        channel = data.get("channel")

                        if channel == "pong":
                            continue

                        if channel == "webData2":
                            update_entry_price_cache(data.get("data", {}), entry_px_by_coin)
                            continue

                        if channel != "userFills":
                            continue

                        payload = data.get("data", {})
                        fills = payload.get("fills", [])

                        if payload.get("isSnapshot"):
                            # Initial batch is trade history on connect, not new
                            # activity — record it but don't alert retroactively.
                            sys.stdout.write(
                                f"[INFO] Baseline fill history received: {len(fills)} fill(s)\n"
                            )
                            sys.stdout.flush()
                            continue

                        for fill in fills:
                            alert_text = build_fill_alert(fill, entry_px_by_coin)
                            await send_telegram_message(session, alert_text)

                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            finally:
                ping_task.cancel()

async def main():
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
