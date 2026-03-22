import os
import json
import time
import datetime
import threading
from typing import Dict, List, Optional, Tuple

import requests
from dateutil import parser
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Timezones
# ---------------------------------------------------------------------------
UTC     = datetime.timezone.utc
CENTRAL = ZoneInfo("America/Chicago")

# ---------------------------------------------------------------------------
# Env vars
# ---------------------------------------------------------------------------
ODDS_API_KEY        = os.getenv("ODDS_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
TWILIO_ACCOUNT_SID  = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN   = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_FROM_NUMBER  = os.getenv("TWILIO_FROM_NUMBER")   # e.g. +15551234567
GSHEETS_SHEET_ID    = os.getenv("GSHEETS_SHEET_ID")     # ID from the Google Sheet URL
GOOGLE_CREDS_JSON   = os.getenv("GOOGLE_CREDS_JSON")    # Service account JSON as a string

# ---------------------------------------------------------------------------
# Odds API config
# ---------------------------------------------------------------------------
SPORT_KEY        = "basketball_ncaab"
SCORES_ENDPOINT  = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/scores"
EVENTS_ENDPOINT  = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/events"

# ---------------------------------------------------------------------------
# Tournament round definitions (Central dates → payout)
# ---------------------------------------------------------------------------
TOURNAMENT_ROUNDS = [
    (datetime.date(2026, 3, 19), datetime.date(2026, 3, 20), "Round of 64",  60),
    (datetime.date(2026, 3, 21), datetime.date(2026, 3, 22), "Round of 32",  125),
    (datetime.date(2026, 3, 26), datetime.date(2026, 3, 27), "Sweet 16",     275),
    (datetime.date(2026, 3, 28), datetime.date(2026, 3, 29), "Elite Eight",  350),
    (datetime.date(2026, 4, 4),  datetime.date(2026, 4, 4),  "Final Four",   500),
    (datetime.date(2026, 4, 6),  datetime.date(2026, 4, 6),  "Championship", 1480),
]

def get_round_info(game_date: datetime.date) -> Tuple[Optional[str], Optional[int]]:
    for start, end, name, payout in TOURNAMENT_ROUNDS:
        if start <= game_date <= end:
            return name, payout
    return None, None

# ---------------------------------------------------------------------------
# Tournament team filter
# ---------------------------------------------------------------------------
def load_tournament_teams(csv_path: str = "tournament-teams.csv") -> set:
    import csv as _csv
    teams = set()
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = _csv.reader(f)
        next(reader)
        for row in reader:
            if row and row[0].strip():
                teams.add(row[0].strip())
    print(f"[INIT] Loaded {len(teams)} tournament teams from {csv_path}", flush=True)
    return teams

TOURNAMENT_TEAMS = load_tournament_teams()

def is_tournament_game(home: str, away: str) -> bool:
    def team_in_field(name: str) -> bool:
        name_lower = name.lower()
        return any(t.lower() in name_lower or name_lower in t.lower() for t in TOURNAMENT_TEAMS)
    return team_in_field(home) or team_in_field(away)

# ---------------------------------------------------------------------------
# Squares grid  (winner_digit, loser_digit) → square_name
# ---------------------------------------------------------------------------
def load_squares_grid(csv_path: str = "square-assignments.csv") -> Dict[tuple, str]:
    import csv as _csv
    grid: Dict[tuple, str] = {}
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = _csv.reader(f)
        rows = list(reader)
    for row in rows[1:]:
        if not row:
            continue
        loser_digit = int(row[0])
        for col_idx in range(1, 11):
            winner_digit = col_idx - 1
            name = row[col_idx].strip() if col_idx < len(row) else ""
            grid[(winner_digit, loser_digit)] = name
    print(f"[INIT] Loaded squares grid from {csv_path} ({len(grid)} squares)", flush=True)
    return grid

SQUARES_GRID = load_squares_grid()

def lookup_square(winner_score: int, loser_score: int) -> Optional[str]:
    key = (winner_score % 10, loser_score % 10)
    return SQUARES_GRID.get(key)

# ---------------------------------------------------------------------------
# Running totals  — persisted to totals.json so restarts don't wipe them
# Key: primary phone number (e.g. "+15551234567")
# ---------------------------------------------------------------------------
TOTALS_FILE = "totals.json"

def load_totals() -> Dict[str, int]:
    if os.path.exists(TOTALS_FILE):
        with open(TOTALS_FILE) as f:
            return json.load(f)
    return {}

def save_totals(totals: Dict[str, int]):
    with open(TOTALS_FILE, "w") as f:
        json.dump(totals, f, indent=2)

# Shared in-memory totals (loaded once, updated on each win)
TOTALS: Dict[str, int] = load_totals()
TOTALS_LOCK = threading.Lock()

def add_to_total(phone: str, amount: int) -> int:
    """Add amount to phone's running total, persist, return new total."""
    with TOTALS_LOCK:
        TOTALS[phone] = TOTALS.get(phone, 0) + amount
        save_totals(TOTALS)
        return TOTALS[phone]

def get_total(phone: str) -> int:
    with TOTALS_LOCK:
        return TOTALS.get(phone, 0)

# ---------------------------------------------------------------------------
# Google Sheets — phone number registry
#
# Expected sheet columns (from Google Form responses):
#   A: Timestamp
#   B: Name (optional — defaults to square name if blank)
#   C: Square name (dropdown)
#   D: Primary phone
#   E: Secondary phone (optional)
#   F: Report an error / Leave a note (ignored by code)
#
# Registry built from this: (winner_digit, loser_digit) →
#   {"display_name": str, "phones": [str, ...]}
# One person may appear multiple times (multiple squares) — all map to same phones.
# ---------------------------------------------------------------------------

# Thread-safe registry updated every 30 minutes
PHONE_REGISTRY: Dict[tuple, Dict] = {}
REGISTRY_LOCK  = threading.Lock()

def _normalize_phone(raw: str) -> Optional[str]:
    """Strip formatting and ensure E.164 format (+1XXXXXXXXXX for US numbers)."""
    if not raw:
        return None
    digits = "".join(c for c in raw if c.isdigit())
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None  # unrecognizable format — skip

def _square_name_to_digits(square_name: str) -> Optional[Tuple[int, int]]:
    """
    Look up a square name in SQUARES_GRID and return (winner_digit, loser_digit).
    Strips the digit hint from the dropdown label if present,
    e.g. 'Bad Boyz (0,9) & (5,0) & (7,8)' → 'Bad Boyz'
    Returns the FIRST matching square (registration handles multi-square owners).
    """
    import re
    # Strip everything from the first '(' onward to get the bare name
    bare = re.sub(r'\s*\(.*', '', square_name).strip()
    for (w, l), name in SQUARES_GRID.items():
        if name.strip().lower() == bare.lower():
            return (w, l)
    return None

def _square_name_to_all_digits(square_name: str) -> List[Tuple[int, int]]:
    """
    Return ALL (winner_digit, loser_digit) squares owned by this name.
    Used so multi-square owners get registered for every square they own.
    """
    import re
    bare = re.sub(r'\s*\(.*', '', square_name).strip()
    return [(w, l) for (w, l), name in SQUARES_GRID.items()
            if name.strip().lower() == bare.lower()]

def fetch_phone_registry() -> Dict[tuple, Dict]:
    """
    Pull the Google Sheet and build the phone registry.
    Returns a dict keyed by (winner_digit, loser_digit).
    """
    if not GSHEETS_SHEET_ID or not GOOGLE_CREDS_JSON:
        print("[SHEETS] Missing GSHEETS_SHEET_ID or GOOGLE_CREDS_JSON — SMS disabled.", flush=True)
        return {}

    try:
        import gspread
        from google.oauth2.service_account import Credentials

        creds_info = json.loads(GOOGLE_CREDS_JSON)
        scopes     = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds      = Credentials.from_service_account_info(creds_info, scopes=scopes)
        gc         = gspread.authorize(creds)
        sheet      = gc.open_by_key(GSHEETS_SHEET_ID).sheet1
        rows       = sheet.get_all_values()

        if len(rows) < 2:
            print("[SHEETS] Sheet has no responses yet.", flush=True)
            return {}

        registry: Dict[tuple, Dict] = {}

        for row in rows[1:]:  # skip header
            # Pad row to at least 6 cols
            row = (row + [""] * 6)[:6]
            _, name, sq_name, phone1, phone2, _note = row

            # Resolve phones first — skip row if none valid
            phones = []
            p1 = _normalize_phone(phone1)
            p2 = _normalize_phone(phone2)
            if p1:
                phones.append(p1)
            if p2 and p2 not in phones:
                phones.append(p2)
            if not phones:
                continue

            # Register ALL squares owned by this name (handles multi-square owners)
            keys = _square_name_to_all_digits(sq_name)
            if not keys:
                print(f"[SHEETS] Could not match square name '{sq_name}' to grid — skipping.", flush=True)
                continue

            for key in keys:
                # Display name defaults to square name from grid if left blank
                display = name.strip() or SQUARES_GRID.get(key, f"Square {key}")
                # Latest submission wins for each square
                registry[key] = {"display_name": display, "phones": phones}

        print(f"[SHEETS] Loaded {len(registry)} square phone registrations.", flush=True)
        return registry

    except Exception as e:
        print(f"[SHEETS] Error fetching registry: {e}", flush=True)
        return {}

def refresh_registry_loop(interval_seconds: int = 1800):
    """Background thread: refresh phone registry every 30 minutes."""
    global PHONE_REGISTRY
    while True:
        time.sleep(interval_seconds)
        new_registry = fetch_phone_registry()
        with REGISTRY_LOCK:
            PHONE_REGISTRY.update(new_registry)
        print(f"[SHEETS] Registry refreshed — {len(PHONE_REGISTRY)} registrations.", flush=True)

def get_registration(winner_digit: int, loser_digit: int) -> Optional[Dict]:
    with REGISTRY_LOCK:
        return PHONE_REGISTRY.get((winner_digit, loser_digit))

# ---------------------------------------------------------------------------
# Twilio SMS
# ---------------------------------------------------------------------------
def send_sms(to: str, body: str):
    """Send a single SMS via Twilio REST API."""
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER]):
        print(f"[SMS] Twilio env vars missing — skipping SMS to {to}", flush=True)
        return
    url  = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
    data = {"From": TWILIO_FROM_NUMBER, "To": to, "Body": body}
    try:
        resp = requests.post(url, data=data, auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN))
        resp.raise_for_status()
        print(f"[SMS] Sent to {to}: {body[:60]}...", flush=True)
    except Exception as e:
        print(f"[SMS] Error sending to {to}: {e}", flush=True)

# ---------------------------------------------------------------------------
# Datetime helpers
# ---------------------------------------------------------------------------
def iso_to_utc_dt(iso_str: str) -> datetime.datetime:
    return parser.isoparse(iso_str).astimezone(UTC)

def format_game_time_central(start_iso: str) -> str:
    return iso_to_utc_dt(start_iso).astimezone(CENTRAL).strftime("%Y-%m-%d %I:%M %p %Z")

def now_central_str() -> str:
    return datetime.datetime.now(UTC).astimezone(CENTRAL).strftime("%Y-%m-%d %I:%M:%S %p %Z")

# ---------------------------------------------------------------------------
# Odds API helpers
# ---------------------------------------------------------------------------
def fetch_todays_schedule() -> List[Dict]:
    """Fetch today's tournament games from the /events endpoint (upcoming only, no score data)."""
    params = {"apiKey": ODDS_API_KEY}
    resp   = requests.get(EVENTS_ENDPOINT, params=params)
    resp.raise_for_status()
    data   = resp.json()

    today_central = datetime.datetime.now(CENTRAL).date()
    games: List[Dict] = []

    for g in data:
        commence = g.get("commence_time")
        if not commence:
            continue
        central_start = iso_to_utc_dt(commence).astimezone(CENTRAL)
        if central_start.date() != today_central:
            continue
        if not is_tournament_game(g["home_team"], g["away_team"]):
            print(f"[FILTER] Skipping non-tournament event: {g['home_team']} vs {g['away_team']}", flush=True)
            continue
        games.append({
            "id":          g["id"],
            "home":        g["home_team"],
            "away":        g["away_team"],
            "start_time":  commence,
            "poll_active": False,
        })

    print(f"[SCHEDULE] {now_central_str()} Loaded {len(games)} tournament games for today.", flush=True)
    for game in games:
        print(f"[SCHEDULE]   {game['home']} vs {game['away']} at {format_game_time_central(game['start_time'])}", flush=True)
    return games

def should_start_polling(start_iso: str, delay_hours: float = 1.5) -> bool:
    poll_start = iso_to_utc_dt(start_iso) + datetime.timedelta(hours=delay_hours)
    return datetime.datetime.now(UTC) >= poll_start

def poll_all_games(days_from: int = 1) -> List[Dict]:
    params = {"apiKey": ODDS_API_KEY, "daysFrom": days_from}
    resp   = requests.get(SCORES_ENDPOINT, params=params)
    resp.raise_for_status()
    print(f"[ODDS] Remaining={resp.headers.get('x-requests-remaining')}  "
          f"Used={resp.headers.get('x-requests-used')}  "
          f"LastCost={resp.headers.get('x-requests-last')}", flush=True)
    return resp.json()

def find_finished_for_game(game: Dict, all_scores: List[Dict]) -> Optional[Dict]:
    for g in all_scores:
        if g.get("id") != game["id"]:
            continue
        # Must have completed=True AND a scores array — avoids triggering on live games
        if g.get("completed") is True and g.get("scores"):
            return {
                "id":         g["id"],
                "home":       g["home_team"],
                "away":       g["away_team"],
                "completed":  True,
                "scores":     g["scores"],
                "start_time": g.get("commence_time"),
            }
        elif g.get("scores") and not g.get("completed"):
            print(f"[DEBUG] {g['home_team']} vs {g['away_team']} has scores but completed=False — still in progress, skipping.", flush=True)
    return None

# ---------------------------------------------------------------------------
# Notifications (Discord + SMS)
# ---------------------------------------------------------------------------
def notify_game_result(game: Dict):
    """Parse final score, post to Discord, and SMS the square winner."""
    scores     = game.get("scores", [])
    home_score = next((int(s["score"]) for s in scores if s["name"] == game["home"]), None)
    away_score = next((int(s["score"]) for s in scores if s["name"] == game["away"]), None)

    if home_score is None or away_score is None:
        print(f"[NOTIFY] Could not parse scores for {game['home']} vs {game['away']}, skipping.", flush=True)
        return

    # Winner / loser
    if home_score >= away_score:
        winner_name, winner_pts = game["home"], home_score
        loser_name,  loser_pts  = game["away"], away_score
    else:
        winner_name, winner_pts = game["away"], away_score
        loser_name,  loser_pts  = game["home"], home_score

    winner_digit = winner_pts % 10
    loser_digit  = loser_pts  % 10

    # Square owner name (from grid CSV)
    square_name = lookup_square(winner_pts, loser_pts) or "unowned square"

    # Round + payout
    game_date  = iso_to_utc_dt(game["start_time"]).astimezone(CENTRAL).date()
    round_name, payout = get_round_info(game_date)
    round_text  = round_name or "Tournament"
    payout_text = f"${payout:,}" if payout else "N/A"

    tip_central = format_game_time_central(game["start_time"]) if game.get("start_time") else "unknown"

    # ── Discord ──────────────────────────────────────────────────────────────
    discord_msg = (
        f"✅ **NCAAB Final ({round_text})** | "
        f"{winner_name} **{winner_pts}** - {loser_name} **{loser_pts}** "
        f"(tip: {tip_central})\n"
        f"🏆 Square **({winner_digit}, {loser_digit})** wins **{payout_text}** — congrats {square_name}!"
    )
    _send_discord(discord_msg)

    # ── SMS ──────────────────────────────────────────────────────────────────
    if payout:
        registration = get_registration(winner_digit, loser_digit)
        if registration:
            display_name = registration["display_name"]
            for phone in registration["phones"]:
                new_total = add_to_total(phone, payout)
                sms_body  = (
                    f"🏆 {display_name} — your square ({winner_digit},{loser_digit}) won!\n"
                    f"{winner_name} {winner_pts} - {loser_name} {loser_pts} ({round_text})\n"
                    f"You won ${payout:,}! Tournament total: ${new_total:,}"
                )
                send_sms(phone, sms_body)
        else:
            print(f"[SMS] No phone registration found for square ({winner_digit},{loser_digit}) — {square_name}", flush=True)


def _send_discord(content: str):
    if not DISCORD_WEBHOOK_URL:
        return
    max_retries = 3
    base_delay  = 2.0
    for attempt in range(1, max_retries + 1):
        resp = requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", base_delay))
            print(f"[DISCORD] 429 rate limited, waiting {wait}s (attempt {attempt}/{max_retries})", flush=True)
            time.sleep(wait)
            continue
        try:
            resp.raise_for_status()
            print(f"[DISCORD] Sent: {content[:80]}...", flush=True)
        except requests.HTTPError as e:
            print(f"[DISCORD] Error: {e}", flush=True)
        break
    time.sleep(0.5)

# ---------------------------------------------------------------------------
# Backfill: post results for already-completed games
# ---------------------------------------------------------------------------
def run_backfill(last_send_time: datetime.datetime, send_interval: datetime.timedelta) -> datetime.datetime:
    print(f"[BACKFILL] {now_central_str()} Fetching completed games from past 3 days...", flush=True)
    try:
        all_scores = poll_all_games(days_from=3)
    except Exception as e:
        print(f"[BACKFILL] Error fetching scores: {e}", flush=True)
        return last_send_time

    first_round_start = datetime.date(2026, 3, 19)
    completed = [
        g for g in all_scores
        if g.get("completed") is True
        and g.get("scores")
        and is_tournament_game(g["home_team"], g["away_team"])
        and iso_to_utc_dt(g["commence_time"]).astimezone(CENTRAL).date() >= first_round_start
    ]
    print(f"[BACKFILL] Found {len(completed)} completed tournament games to post.", flush=True)

    for g in completed:
        now_utc = datetime.datetime.now(UTC)
        gap = (now_utc - last_send_time).total_seconds()
        if gap < send_interval.total_seconds():
            wait = send_interval.total_seconds() - gap
            print(f"[BACKFILL] Waiting {wait:.1f}s before next send...", flush=True)
            time.sleep(wait)

        notify_game_result({
            "id":         g["id"],
            "home":       g["home_team"],
            "away":       g["away_team"],
            "status":     g.get("status"),
            "scores":     g["scores"],
            "start_time": g.get("commence_time"),
        })
        last_send_time = datetime.datetime.now(UTC)

    print(f"[BACKFILL] Done.", flush=True)
    return last_send_time

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    if not ODDS_API_KEY or not DISCORD_WEBHOOK_URL:
        raise RuntimeError("Missing ODDS_API_KEY or DISCORD_WEBHOOK_URL")

    # Startup Discord notification
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": "🚀 NCAAB notifier started and is now polling."}).raise_for_status()
        print("[DISCORD] Sent startup notification", flush=True)
    except Exception as e:
        print(f"[DISCORD] Failed startup notification: {e}", flush=True)

    # Initial Google Sheets load
    global PHONE_REGISTRY
    PHONE_REGISTRY = fetch_phone_registry()

    # Background thread: refresh sheet every 30 minutes
    t = threading.Thread(target=refresh_registry_loop, args=(1800,), daemon=True)
    t.start()
    print("[SHEETS] Background registry refresh thread started (every 30 min).", flush=True)

    # Rate-limit tracker (shared across backfill + live loop)
    last_send_time = datetime.datetime.min.replace(tzinfo=UTC)
    send_interval  = datetime.timedelta(seconds=20)

    # Backfill completed games (runs once at startup)
    last_send_time = run_backfill(last_send_time, send_interval)

    # Outer loop: check once per day whether there are tournament games
    while True:
        print(f"[SCHEDULE] {now_central_str()} Checking today's schedule...", flush=True)
        try:
            games: List[Dict] = fetch_todays_schedule()
        except Exception as e:
            print(f"[SCHEDULE] Error fetching schedule: {e} — retrying in 1 hour.", flush=True)
            time.sleep(3600)
            continue

        if not games:
            # Sleep until 9am Central tomorrow
            now_central  = datetime.datetime.now(CENTRAL)
            tomorrow_9am = (now_central + datetime.timedelta(days=1)).replace(
                hour=9, minute=0, second=0, microsecond=0
            )
            sleep_secs = (tomorrow_9am - now_central).total_seconds()
            print(f"[SCHEDULE] No tournament games today — sleeping until {tomorrow_9am.strftime('%Y-%m-%d 9:00 AM %Z')} ({sleep_secs/3600:.1f}h).", flush=True)
            time.sleep(sleep_secs)
            continue

        # Inner loop: poll until all today's games are completed
        print(f"[SCHEDULE] {len(games)} game(s) today — entering polling loop.", flush=True)
        while games:
            current_minute = datetime.datetime.now(UTC).minute
            if current_minute % 2 == 0:
                print("=== Polling loop tick ===", flush=True)
                print(f"[GAMES] {len(games)} games remaining", flush=True)

                try:
                    all_scores = poll_all_games(days_from=2)
                except Exception as e:
                    print(f"[ODDS] Error: {e}", flush=True)
                    time.sleep(60)
                    continue

                for game in list(games):  # copy so removal mid-loop is safe
                    if should_start_polling(game["start_time"], delay_hours=1.833):
                        if not game["poll_active"]:
                            print(f"[GAMES] Activating polling for {game['home']} vs {game['away']}", flush=True)
                            game["poll_active"] = True
                    else:
                        print(f"[GAMES] Not yet time to poll {game['home']} vs {game['away']}", flush=True)
                        continue

                    finished = find_finished_for_game(game, all_scores)
                    if finished:
                        now_utc = datetime.datetime.now(UTC)
                        if now_utc - last_send_time < send_interval:
                            print("[NOTIFY] Skipping this tick — respecting 20s interval", flush=True)
                            continue

                        print(f"[GAMES] Finished: {finished['home']} vs {finished['away']}", flush=True)
                        notify_game_result(finished)
                        games.remove(game)
                        last_send_time = datetime.datetime.now(UTC)
                        break  # one notification per tick
                    else:
                        print(f"[DEBUG] {game['home']} vs {game['away']} no scores yet", flush=True)

            print(f"[HEARTBEAT] {now_central_str()} Loop complete", flush=True)
            time.sleep(60)

        # All games done for today — sleep until 9am Central tomorrow
        now_central  = datetime.datetime.now(CENTRAL)
        tomorrow_9am = (now_central + datetime.timedelta(days=1)).replace(
            hour=9, minute=0, second=0, microsecond=0
        )
        sleep_secs = (tomorrow_9am - now_central).total_seconds()
        print(f"[SCHEDULE] All games complete for today — sleeping until {tomorrow_9am.strftime('%Y-%m-%d 9:00 AM %Z')} ({sleep_secs/3600:.1f}h).", flush=True)
        time.sleep(sleep_secs)


if __name__ == "__main__":
    main()
