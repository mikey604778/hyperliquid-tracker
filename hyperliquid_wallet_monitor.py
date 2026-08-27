import asyncio
import aiohttp
import json
import os
import random
import sys

# ==============================================================================
# ⚙️ CONFIGURATION & CREDENTIALS
# ==============================================================================
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"

# Track an observed account, configured via Railway environment variable
TARGET_ADDRESS = os.environ["HYPERLIQUID_WALLET_ADDRESS"]

# Reconnect backoff bounds (seconds) -- see main().
RECONNECT_BASE_DELAY = 1
RECONNECT_MAX_DELAY = 60

# How long to wait, after webData2 shows a position size we haven't accounted for via a
# real userFills alert, before trusting that userFills truly dropped it and firing a
# synthetic fallback alert instead. Long enough to let an in-flight fill message land,
# short enough that the fallback still reads as "real time".
FALLBACK_GRACE_SECONDS = 4.0

# Where the position cache is persisted across restarts, so a Railway redeploy/crash
# doesn't silently re-baseline (and thus swallow) a position change that happened while
# the process was down. Relative to the working directory the service runs from.
STATE_FILE_PATH = os.environ.get("TRACKER_STATE_FILE", "tracker_state.json")

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


def _load_persisted_state() -> dict | None:
    """Reads the on-disk position snapshot from the previous run, if any. Returns None on
    a fresh install/first-ever boot (no file) or a corrupt/unreadable file -- either way
    the caller falls back to a normal cold bootstrap."""
    try:
        with open(STATE_FILE_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except (json.JSONDecodeError, OSError) as e:
        sys.stderr.write(f"[WARN] Could not read {STATE_FILE_PATH}: {e}. Starting cold.\n")
        sys.stderr.flush()
        return None


def save_state(state: dict) -> None:
    """Persists the position cache to disk so a container restart/crash-loop can restore
    it instead of silently re-baselining on the next webData2 snapshot. Written atomically
    (temp file + os.replace) so a crash mid-write never corrupts the file that's read on
    the next boot. Best-effort: a write failure is logged, never fatal."""
    snapshot = {
        "entry_by_coin": state["entry_by_coin"],
        "last_entry_by_coin": state["last_entry_by_coin"],
        "size_by_coin": state["size_by_coin"],
        "mark_by_coin": state["mark_by_coin"],
        "confirmed_size_by_coin": state["confirmed_size_by_coin"],
    }
    tmp_path = f"{STATE_FILE_PATH}.tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(snapshot, f)
        os.replace(tmp_path, STATE_FILE_PATH)
    except OSError as e:
        sys.stderr.write(f"[WARN] Could not persist state to {STATE_FILE_PATH}: {e}\n")
        sys.stderr.flush()


def new_tracker_state() -> dict:
    """Creates the persistent cross-reconnect state blob threaded through the whole run.
    Living at main()'s level (not inside handle_websocket_stream) means a dropped/rebuilt
    WebSocket connection never resets it -- so if userFills genuinely drops a fill while
    disconnected, the very next webData2 snapshot after reconnect still catches the size
    mismatch and the fallback engine below fires for it.

    Also restores from STATE_FILE_PATH (written by save_state) when present, so a Railway
    redeploy or crash-loop restart -- which wipes the in-memory dict above -- doesn't lose
    the account's last-known position state too. A coin restored this way is pre-marked
    "bootstrapped" with its saved size as the confirmed baseline, so the very first
    webData2 snapshot after boot runs through the normal mismatch check (not the silent
    first-sighting bootstrap path) and fires a real fallback alert if the position moved
    while the process was down."""
    disk_state = _load_persisted_state()
    state = {
        "entry_by_coin": {},       # coin -> current avg entry px (deleted when flat, like before)
        "last_entry_by_coin": {},  # coin -> most recent non-null avg entry px (never deleted;
                                    # needed as the PNL basis for a fallback alert that fires
                                    # exactly when a position goes flat and entry_by_coin clears)
        "size_by_coin": {},        # coin -> signed size (szi) from the latest webData2 snapshot
        "mark_by_coin": {},        # coin -> approx mark price, derived from positionValue/|szi|
        "confirmed_size_by_coin": {},  # coin -> signed size already accounted for by an alert
                                        # (real fill or synthetic fallback)
        "bootstrapped": set(),     # coins whose first-ever snapshot we've baselined (skip
                                    # alerting on process startup for pre-existing positions)
        "pending_fallback": {},    # coin -> in-flight asyncio.Task from schedule_fallback_check
    }

    if disk_state:
        state["entry_by_coin"].update(disk_state.get("entry_by_coin", {}))
        state["last_entry_by_coin"].update(disk_state.get("last_entry_by_coin", {}))
        state["size_by_coin"].update(disk_state.get("size_by_coin", {}))
        state["mark_by_coin"].update(disk_state.get("mark_by_coin", {}))
        state["confirmed_size_by_coin"].update(disk_state.get("confirmed_size_by_coin", {}))
        state["bootstrapped"].update(state["confirmed_size_by_coin"].keys())
        sys.stdout.write(
            f"[INFO] Restored position state from {STATE_FILE_PATH} for coins: "
            f"{sorted(state['confirmed_size_by_coin'].keys())}\n"
        )
        sys.stdout.flush()

    return state


def update_position_cache(web_data2: dict, state: dict) -> None:
    """Refreshes size/entry/mark caches from a webData2 snapshot, in place. This is the
    state-fallback engine's data source (see maybe_schedule_fallback / build_fallback_alert
    below) in addition to still backing build_fill_alert's avg-entry lookups."""
    entry_by_coin = state["entry_by_coin"]
    last_entry_by_coin = state["last_entry_by_coin"]
    size_by_coin = state["size_by_coin"]
    mark_by_coin = state["mark_by_coin"]

    seen_coins = set()
    for entry in web_data2.get("clearinghouseState", {}).get("assetPositions", []):
        pos = entry.get("position", {})
        coin = pos.get("coin")
        if not coin:
            continue
        seen_coins.add(coin)

        entry_px = pos.get("entryPx")
        if entry_px is not None:
            entry_by_coin[coin] = float(entry_px)
            last_entry_by_coin[coin] = float(entry_px)
        elif coin in entry_by_coin:
            del entry_by_coin[coin]

        szi = pos.get("szi")
        if szi is not None:
            szi_f = float(szi)
            size_by_coin[coin] = szi_f
            position_value = pos.get("positionValue")
            if position_value is not None and szi_f != 0:
                try:
                    mark_by_coin[coin] = abs(float(position_value) / szi_f)
                except (TypeError, ValueError, ZeroDivisionError):
                    pass

    # Any coin that previously had an open position but no longer appears at all in this
    # snapshot has gone fully flat -- reflect that as size 0 so the mismatch check below
    # (and a full-close fallback alert, if userFills missed it) still works correctly.
    for coin in list(size_by_coin.keys()):
        if coin not in seen_coins:
            size_by_coin[coin] = 0.0


# Kept as a thin alias so nothing else needs to change: some call sites only care about
# the avg-entry side of the cache.
def update_entry_price_cache(web_data2: dict, entry_px_by_coin: dict) -> None:
    for entry in web_data2.get("clearinghouseState", {}).get("assetPositions", []):
        pos = entry.get("position", {})
        coin = pos.get("coin")
        entry_px = pos.get("entryPx")
        if coin and entry_px is not None:
            entry_px_by_coin[coin] = float(entry_px)
        elif coin and coin in entry_px_by_coin:
            del entry_px_by_coin[coin]


def build_fill_alert(fill: dict, entry_px_by_coin: dict) -> str:
    """Formats a single userFills entry exactly like the reference tracker bot.

    Hyperliquid fill fields used: coin, px (fill price), sz (fill size),
    side ("B"=buy/"A"=sell), dir (human label e.g. "Open Long", "Close Short"),
    startPosition (signed position size *before* this fill), closedPnl (realized PNL
    on this fill -- 0/absent on opens and same-direction adds).

    Avg entry price comes from the side-by-side webData2 snapshot cache since
    individual fills don't carry the account's running average entry price.

    Any size change reported by this fill (open, add, reduce, close, liquidation --
    Hyperliquid tags all of these on the same userFills stream) is rendered here; there
    is no dir-based allow-list that could silently drop one.
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

    # Reduce or full close (also covers liquidations -- Hyperliquid reports those as
    # ordinary fills with dir like "Liquidated Short", which falls into this branch too).
    # The side being reduced is the position's side *before* this fill (start_position's
    # sign) -- new_position's sign/word is meaningless on a full close since it's 0 there.
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


# ==============================================================================
# STATE FALLBACK ENGINE (webData2-driven safety net for dropped userFills messages)
# ==============================================================================
# userFills is the fast path for alerts; webData2 is the source of truth for the
# account's actual position size. If the two ever disagree -- webData2 shows a size
# userFills never told us about -- that means a fill was dropped somewhere between
# Hyperliquid and us (a reconnect gap, a missed WS frame, etc). This engine detects
# that disagreement and synthesizes the missing alert from webData2 alone.

def build_fallback_alert(coin: str, old_szi: float, new_szi: float, state: dict) -> str | None:
    """Builds a synthetic Telegram alert for a position-size change that webData2 observed
    but no userFills message accounted for. Execution price is approximated from the
    position's mark price (positionValue / |szi|, cached by update_position_cache) since
    webData2 carries no actual fill price. Returns None if there isn't enough data to
    safely render an alert (better to log-only than to guess a garbage number)."""
    old_sign = _sign(old_szi)
    new_sign = _sign(new_szi)
    mark_px = state["mark_by_coin"].get(coin)
    entry_px = state["entry_by_coin"].get(coin) or state["last_entry_by_coin"].get(coin)
    if mark_px is None or mark_px <= 0:
        mark_px = entry_px
    if mark_px is None or mark_px <= 0:
        return None

    line1 = f"whale 1 \\({md_escape(_short_addr(TARGET_ADDRESS))}\\)"

    # Case 1: reduce or full close -- magnitude shrank, same side (or now fully flat).
    if old_sign != 0 and abs(new_szi) < abs(old_szi) - 1e-9 and (new_sign == old_sign or new_szi == 0):
        side_word = "LONG" if old_sign > 0 else "SHORT"
        size_closed = abs(old_szi) - abs(new_szi)
        avg_entry = state["last_entry_by_coin"].get(coin, mark_px)
        dir_factor = -1 if side_word == "SHORT" else 1
        pnl = (mark_px - avg_entry) * size_closed * dir_factor
        is_full_close = abs(new_szi) < 1e-9
        action_text = f"{'Closed' if is_full_close else 'Reduced'} {coin} {side_word}"
        pnl_icon = "✅" if pnl >= 0 else "❌"
        pnl_sign = "+" if pnl >= 0 else "-"
        notional = abs(new_szi * mark_px)
        line2 = f"{md_escape(action_text)} by {md_escape(_fmt_num(size_closed))} @ ${md_escape(_fmt_num(mark_px))}"
        line3 = f"Closed PNL: {pnl_sign}${md_escape(_fmt_num(abs(pnl)))} {pnl_icon}"
        line4 = (
            f"Remaining Size: {md_escape(_fmt_num(abs(new_szi)))} "
            f"\\({md_escape(_fmt_usd(notional))}\\) \\| "
            f"Avg Entry: ${md_escape(_fmt_num(avg_entry))}"
        )
        return f"{line1}\n{line2}\n{line3}\n{line4}"

    # Case 2: scale-in / add (same side, magnitude grew) or a brand-new open.
    if new_sign != 0 and (old_sign == 0 or new_sign == old_sign) and abs(new_szi) > abs(old_szi) + 1e-9:
        side_word = "LONG" if new_sign > 0 else "SHORT"
        icon = "🟢" if side_word == "LONG" else "🔴"
        size_added = abs(new_szi) - abs(old_szi)
        action_text = f"{'Open' if old_sign == 0 else 'Added'} {side_word.capitalize()}"
        notional = abs(new_szi * mark_px)
        line2 = f"{md_escape(action_text)} {md_escape(coin)} {icon} by {md_escape(_fmt_num(size_added))} @ {md_escape(_fmt_num(mark_px))}"
        line3 = (
            f"Total Size: {md_escape(_fmt_num(abs(new_szi)))} "
            f"\\({md_escape(_fmt_usd(notional))}\\) \\| "
            f"Avg Entry: {md_escape(_fmt_num(entry_px or mark_px))}"
        )
        return f"{line1}\n{line2}\n{line3}"

    # Case 3: direction flip in one webData2 tick (short -> long or vice versa). Report
    # the old side's full close here; if the new side's size sticks around, the next
    # mismatch check will separately catch it as a fresh "Open" via case 2 above.
    if old_sign != 0 and new_sign != 0 and old_sign != new_sign:
        side_word = "LONG" if old_sign > 0 else "SHORT"
        avg_entry = state["last_entry_by_coin"].get(coin, mark_px)
        dir_factor = -1 if side_word == "SHORT" else 1
        pnl = (mark_px - avg_entry) * abs(old_szi) * dir_factor
        pnl_icon = "✅" if pnl >= 0 else "❌"
        pnl_sign = "+" if pnl >= 0 else "-"
        line2 = f"{md_escape(f'Closed {coin} {side_word}')} by {md_escape(_fmt_num(abs(old_szi)))} @ ${md_escape(_fmt_num(mark_px))}"
        line3 = f"Closed PNL: {pnl_sign}${md_escape(_fmt_num(abs(pnl)))} {pnl_icon}"
        line4 = "Remaining Size: 0 \\(\\$0\\.00\\) \\| Avg Entry: " + f"${md_escape(_fmt_num(avg_entry))}"
        return f"{line1}\n{line2}\n{line3}\n{line4}"

    return None


async def _fallback_check_after_delay(coin: str, state: dict, session: aiohttp.ClientSession):
    """Waits FALLBACK_GRACE_SECONDS, then re-checks whether the size mismatch on `coin`
    is still unresolved. A real userFills alert arriving in the meantime updates
    confirmed_size_by_coin itself and self-cancels this fallback -- so on the happy path
    (no dropped fill) this coroutine does nothing but sleep and exit."""
    try:
        await asyncio.sleep(FALLBACK_GRACE_SECONDS)
        new_szi = state["size_by_coin"].get(coin, 0.0)
        confirmed = state["confirmed_size_by_coin"].get(coin, new_szi)
        if abs(new_szi - confirmed) < 1e-9:
            return  # a real fill alert already reconciled this -- nothing was dropped
        sys.stdout.write(
            f"[FALLBACK] webData2 saw an un-alerted size change on {coin}: "
            f"{confirmed} -> {new_szi} -- userFills likely dropped a fill. Synthesizing alert.\n"
        )
        sys.stdout.flush()
        alert_text = build_fallback_alert(coin, confirmed, new_szi, state)
        if alert_text:
            await send_telegram_message(session, alert_text)
        state["confirmed_size_by_coin"][coin] = new_szi
        save_state(state)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        sys.stderr.write(f"[ERROR] Fallback check failed for {coin}: {str(e)}\n")
        sys.stderr.flush()
    finally:
        state["pending_fallback"].pop(coin, None)


def maybe_schedule_fallback(coin: str, state: dict, session: aiohttp.ClientSession) -> None:
    """Called after every webData2 update for `coin`. Bootstraps a first-ever sighting of
    the coin silently (so restarting the process against an already-open position doesn't
    fire a bogus alert), then schedules (at most one in-flight) delayed re-check whenever
    the live size disagrees with what's already been alerted on."""
    new_szi = state["size_by_coin"].get(coin, 0.0)

    if coin not in state["bootstrapped"]:
        state["bootstrapped"].add(coin)
        state["confirmed_size_by_coin"][coin] = new_szi
        return

    confirmed = state["confirmed_size_by_coin"].get(coin, new_szi)
    if abs(new_szi - confirmed) < 1e-9:
        return

    existing_task = state["pending_fallback"].get(coin)
    if existing_task and not existing_task.done():
        return  # already watching this coin's mismatch

    state["pending_fallback"][coin] = asyncio.create_task(
        _fallback_check_after_delay(coin, state, session)
    )


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

async def handle_websocket_stream(state: dict):
    entry_px_by_coin = state["entry_by_coin"]

    async with aiohttp.ClientSession() as session:
        sys.stdout.write("[INFO] Connecting to Hyperliquid WebSocket Pipeline...\n")
        sys.stdout.flush()

        async with session.ws_connect(HYPERLIQUID_WS_URL) as ws:
            sys.stdout.write("[INFO] WebSocket Pipeline Connected Successfully!\n")
            sys.stdout.flush()

            # userFills gives instant per-fill alerts; webData2 runs alongside both to
            # keep an up-to-date avg entry price per coin (individual fills don't carry
            # the account's running avg entry) AND to drive the state-fallback engine
            # above, which catches any fill userFills itself failed to deliver.
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
                            web_data2 = data.get("data", {})
                            update_position_cache(web_data2, state)
                            for coin in list(state["size_by_coin"].keys()):
                                maybe_schedule_fallback(coin, state, session)
                            save_state(state)
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

                        # Every fill in the payload is alerted on -- opens, adds, partial
                        # reduces, full closes, and liquidations alike. There is no dir-based
                        # filter here that could silently drop one.
                        for fill in fills:
                            alert_text = build_fill_alert(fill, entry_px_by_coin)
                            await send_telegram_message(session, alert_text)

                            # Mark this size change as accounted for, so the fallback
                            # engine (driven by the next webData2 tick) doesn't also
                            # fire a duplicate synthetic alert for the same fill.
                            coin = fill.get("coin")
                            if coin:
                                sz = float(fill.get("sz", 0))
                                side = fill.get("side", "B")
                                start_position = float(fill.get("startPosition", 0) or 0)
                                signed_sz = sz if side == "B" else -sz
                                state["confirmed_size_by_coin"][coin] = start_position + signed_sz
                                save_state(state)

                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            finally:
                ping_task.cancel()
                for task in state["pending_fallback"].values():
                    task.cancel()

async def main():
    state = new_tracker_state()
    backoff = RECONNECT_BASE_DELAY

    while True:
        try:
            await handle_websocket_stream(state)
            # A clean return from handle_websocket_stream (WS closed/errored without
            # raising) still means we lost the connection -- reconnect immediately at
            # the base delay rather than waiting on a stale backoff from a prior failure.
            backoff = RECONNECT_BASE_DELAY
        except Exception as e:
            jittered_delay = backoff * random.uniform(0.8, 1.2)
            sys.stderr.write(
                f"[ERROR] WebSocket disconnected: {str(e)}. "
                f"Reconnecting in {jittered_delay:.1f}s (exponential backoff)...\n"
            )
            sys.stderr.flush()
            await asyncio.sleep(jittered_delay)
            backoff = min(RECONNECT_MAX_DELAY, backoff * 2)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
