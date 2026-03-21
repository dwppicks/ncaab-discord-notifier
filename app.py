import os
import time
import datetime
from typing import Dict, List, Optional

import requests
from dateutil import parser
from zoneinfo import ZoneInfo  # Python 3.9+

# ---------------------------------------------------------------------------
# Timezones
# ---------------------------------------------------------------------------
UTC = datetime.timezone.utc
CENTRAL = ZoneInfo("America/Chicago")

# ---------------------------------------------------------------------------
# Env vars
# ---------------------------------------------------------------------------
ODDS_API_KEY = os.getenv("ODDS_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# ---------------------------------------------------------------------------
# Odds API config
# ---------------------------------------------------------------------------
SPORT_KEY = "basketball_ncaab"
SCORES_ENDPOINT = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/scores"

# ---------------------------------------------------------------------------
# Tournament round definitions (Central dates → payout)
# ---------------------------------------------------------------------------
# Each entry: (start_date, end_date, round_name, payout)
TOURNAMENT_ROUNDS = [
    (datetime.date(2026, 3, 19), datetime.date(2026, 3, 20), "Round of 64",  60),
    (datetime.date(2026, 3, 21), datetime.date(2026, 3, 22), "Round of 32",  125),
    (datetime.date(2026, 3, 26), datetime.date(2026, 3, 27), "Sweet 16",     275),
    (datetime.date(2026, 3, 28), datetime.date(2026, 3, 29), "Elite Eight",  350),
    (datetime.date(2026, 4, 4),  datetime.date(2026, 4, 4),  "Final Four",   500),
    (datetime.date(2026, 4, 6),  datetime.date(2026, 4, 6),  "Championship", 1480),
]

def get_round_info(game_date: datetime.date):
    """Return (round_name, payout) for a given Central date, or (None, None)."""
    for start, end, name, payout in TOURNAMENT_ROUNDS:
        if start <= game_date <= end:
            return name, payout
    return None, None

# ---------------------------------------------------------------------------
# Tournament team filter
# Loaded from tournament-teams.csv at startup.
# Only games where BOTH teams are in this set will be processed.
# ---------------------------------------------------------------------------
def load_tournament_teams(csv_path: str = "tournament-teams.csv") -> set:
    """Load the set of tournament team names from CSV (one per line, header 'team')."""
    import csv as _csv
    teams = set()
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = _csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if row and row[0].strip():
                teams.add(row[0].strip())
    print(f"[INIT] Loaded {len(teams)} tournament teams from {csv_path}", flush=True)
    return teams

TOURNAMENT_TEAMS = load_tournament_teams()

def is_tournament_game(home: str, away: str) -> bool:
    """Return True if at least one team is in the tournament field."""
    def team_in_field(name: str) -> bool:
        name_lower = name.lower()
        return any(t.lower() in name_lower or name_lower in t.lower() for t in TOURNAMENT_TEAMS)
    return team_in_field(home) or team_in_field(away)

# ---------------------------------------------------------------------------
# Squares grid  (winner_digit, loser_digit) → owner name
# Loaded from square-assignments.csv at startup.
# To update for a new year: just replace the CSV, no code changes needed.
# ---------------------------------------------------------------------------
def load_squares_grid(csv_path: str = "square-assignments.csv") -> Dict[tuple, str]:
    """
    Load the squares grid from a CSV file.
    Row 1 (header): blank, then winner digits 0-9
    Col A: loser digit (0-9)
    Each cell: owner name
    """
    import csv as _csv
    grid: Dict[tuple, str] = {}
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = _csv.reader(f)
        rows = list(reader)
    for row in rows[1:]:  # skip header
        if not row:
            continue
        loser_digit = int(row[0])
        for col_idx in range(1, 11):
            winner_digit = col_idx - 1
            name = row[col_idx].strip() if col_idx < len(row) else ""
            grid[(winner_digit, loser_digit)] = name
    print(f"[INIT] Loaded squares grid from {csv_path} ({len(grid)} squares)", flush=True)
    return grid

# Loaded once at startup — just update square-assignments.csv each year, no code changes needed
SQUARES_GRID = load_squares_grid()

def lookup_square(winner_score: int, loser_score: int) -> Optional[str]:
    """Return the Discord tag for the winning square, or None."""
    key = (winner_score % 10, loser_score % 10)
    return SQUARES_GRID.get(key)

# ---------------------------------------------------------------------------
# Datetime helpers
# ---------------------------------------------------------------------------
def iso_to_utc_dt(iso_str: str) -> datetime.datetime:
    return parser.isoparse(iso_str).astimezone(UTC)

def format_game_time_central(start_iso: str) -> str:
    utc_dt = iso_to_utc_dt(start_iso)
    central_dt = utc_dt.astimezone(CENTRAL)
    return central_dt.strftime("%Y-%m-%d %I:%M %p %Z")

def now_central_str() -> str:
    return datetime.datetime.now(UTC).astimezone(CENTRAL).strftime("%Y-%m-%d %I:%M:%S %p %Z")

# ---------------------------------------------------------------------------
# Odds API helpers
# ---------------------------------------------------------------------------
def fetch_todays_games_central() -> List[Dict]:
    params = {"apiKey": ODDS_API_KEY, "daysFrom": 1}
    resp = requests.get(SCORES_ENDPOINT, params=params)
    resp.raise_for_status()
    data = resp.json()

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
            print(f"[FILTER] Skipping non-tournament game: {g['home_team']} vs {g['away_team']}", flush=True)
            continue
        games.append({
            "id":          g["id"],
            "home":        g["home_team"],
            "away":        g["away_team"],
            "start_time":  commence,
            "poll_active": False,
            "notified":    False,
        })

    print(f"[INIT] {now_central_str()} Fetched {len(games)} games for today (Central date {today_central}).", flush=True)
    for game in games:
        print(f"[INIT] Tracking {game['home']} vs {game['away']} at {format_game_time_central(game['start_time'])}", flush=True)
    return games


def should_start_polling(start_iso: str, delay_hours: float = 1.5) -> bool:
    poll_start = iso_to_utc_dt(start_iso) + datetime.timedelta(hours=delay_hours)
    return datetime.datetime.now(UTC) >= poll_start


def poll_all_games(days_from: int = 1) -> List[Dict]:
    params = {"apiKey": ODDS_API_KEY, "daysFrom": days_from}
    resp = requests.get(SCORES_ENDPOINT, params=params)
    resp.raise_for_status()
    remaining  = resp.headers.get("x-requests-remaining")
    used       = resp.headers.get("x-requests-used")
    last_cost  = resp.headers.get("x-requests-last")
    print(f"[ODDS] Remaining={remaining}, Used={used}, LastCost={last_cost}", flush=True)
    return resp.json()


def find_finished_for_game(game: Dict, all_scores: List[Dict]) -> Optional[Dict]:
    for g in all_scores:
        if g.get("id") != game["id"]:
            continue
        scores = g.get("scores")
        if scores:
            return {
                "id":         g["id"],
                "home":       g["home_team"],
                "away":       g["away_team"],
                "status":     g.get("status"),
                "scores":     scores,
                "start_time": g.get("commence_time"),
            }
    return None

# ---------------------------------------------------------------------------
# Discord notification
# ---------------------------------------------------------------------------
def send_discord_webhook(game: Dict):
    scores      = game.get("scores", [])
    home_score  = next((int(s["score"]) for s in scores if s["name"] == game["home"]), None)
    away_score  = next((int(s["score"]) for s in scores if s["name"] == game["away"]), None)

    if home_score is None or away_score is None:
        print(f"[DISCORD] Could not parse scores for {game['home']} vs {game['away']}, skipping.", flush=True)
        return

    # Determine winner/loser
    if home_score >= away_score:
        winner_name,  winner_pts = game["home"], home_score
        loser_name,   loser_pts  = game["away"], away_score
    else:
        winner_name,  winner_pts = game["away"], away_score
        loser_name,   loser_pts  = game["home"], home_score

    winner_digit = winner_pts % 10
    loser_digit  = loser_pts  % 10

    # Squares lookup
    owner = lookup_square(winner_pts, loser_pts)
    owner_text = owner if owner else "unowned square"

    # Round + payout
    game_date = iso_to_utc_dt(game["start_time"]).astimezone(CENTRAL).date()
    round_name, payout = get_round_info(game_date)
    round_text  = round_name if round_name else "Tournament"
    payout_text = f"${payout:,}" if payout else "N/A"

    tip_central = format_game_time_central(game["start_time"]) if game.get("start_time") else "unknown time"

    content = (
        f"✅ **NCAAB Final ({round_text})** | "
        f"{winner_name} **{winner_pts}** - {loser_name} **{loser_pts}** "
        f"(tip: {tip_central})\n"
        f"🏆 Square **({winner_digit}, {loser_digit})** wins **{payout_text}** — congrats {owner_text}!"
    )

    payload = {"content": content}
    max_retries = 3
    base_delay  = 2.0

    for attempt in range(1, max_retries + 1):
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait = float(retry_after) if retry_after else base_delay
            print(f"[DISCORD] 429 rate limited, waiting {wait}s (attempt {attempt}/{max_retries})", flush=True)
            time.sleep(wait)
            continue
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            print(f"[DISCORD] Error sending webhook: {e}", flush=True)
        else:
            print(f"[DISCORD] Sent notification: {winner_name} {winner_pts} - {loser_name} {loser_pts} | square ({winner_digit},{loser_digit}) → {owner_text}", flush=True)
        break

    time.sleep(0.5)  # small buffer between sends

# ---------------------------------------------------------------------------
# Backfill: post results for already-completed games (e.g. Round of 64)
# ---------------------------------------------------------------------------
def run_backfill(last_send_time: datetime.datetime, send_interval: datetime.timedelta):
    """
    On startup, fetch the last 3 days of scores and post any completed games
    that have a final score. Sends one per 20 seconds. Returns updated last_send_time.
    """
    print(f"[BACKFILL] {now_central_str()} Fetching completed games from past 3 days...", flush=True)
    try:
        # daysFrom=3 pulls games up to 3 days old
        all_scores = poll_all_games(days_from=3)
    except Exception as e:
        print(f"[BACKFILL] Error fetching scores: {e}", flush=True)
        return last_send_time

    first_round_start = datetime.date(2026, 3, 19)
    completed = [
        g for g in all_scores
        if g.get("scores")
        and is_tournament_game(g["home_team"], g["away_team"])
        and iso_to_utc_dt(g["commence_time"]).astimezone(CENTRAL).date() >= first_round_start
    ]
    print(f"[BACKFILL] Found {len(completed)} completed tournament games to post.", flush=True)

    for g in completed:
        game = {
            "id":         g["id"],
            "home":       g["home_team"],
            "away":       g["away_team"],
            "status":     g.get("status"),
            "scores":     g["scores"],
            "start_time": g.get("commence_time"),
        }
        now_utc = datetime.datetime.now(UTC)
        gap = (now_utc - last_send_time).total_seconds()
        if gap < send_interval.total_seconds():
            wait = send_interval.total_seconds() - gap
            print(f"[BACKFILL] Waiting {wait:.1f}s before next send...", flush=True)
            time.sleep(wait)

        send_discord_webhook(game)
        last_send_time = datetime.datetime.now(UTC)

    print(f"[BACKFILL] Done posting {len(completed)} completed games.", flush=True)
    return last_send_time


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    if not ODDS_API_KEY or not DISCORD_WEBHOOK_URL:
        raise RuntimeError("Missing ODDS_API_KEY or DISCORD_WEBHOOK_URL")

    # Startup notification
    try:
        r = requests.post(DISCORD_WEBHOOK_URL, json={"content": "🚀 NCAAB notifier started on Render and is now polling."})
        r.raise_for_status()
        print("[DISCORD] Sent startup notification", flush=True)
    except Exception as e:
        print(f"[DISCORD] Failed to send startup notification: {e}", flush=True)

    # Rate-limit tracker for Discord sends (shared across backfill + live loop)
    last_send_time = datetime.datetime.min.replace(tzinfo=UTC)
    send_interval  = datetime.timedelta(seconds=20)

    # Backfill: post all already-completed games (Round of 64, etc.)
    last_send_time = run_backfill(last_send_time, send_interval)

    # Fetch today's games once
    print(f"[INIT] {now_central_str()} Loading today's games (Central)...", flush=True)
    try:
        games: List[Dict] = fetch_todays_games_central()
    except Exception as e:
        print(f"[INIT] Error fetching today's games: {e}", flush=True)
        games = []

    while True:
        current_minute = datetime.datetime.now(UTC).minute
        if current_minute % 2 == 0 and games:
            print("=== Polling loop tick ===", flush=True)
            active_games = [g for g in games if not g["notified"]]
            print(f"[GAMES] Tracking {len(active_games)} games not yet notified", flush=True)

            try:
                all_scores = poll_all_games()
            except Exception as e:
                print(f"[ODDS] Error calling scores endpoint: {e}", flush=True)
                time.sleep(60)
                continue

            for game in active_games:
                if should_start_polling(game["start_time"]):
                    if not game["poll_active"]:
                        print(f"[GAMES] {now_central_str()} Activating polling for {game['home']} vs {game['away']}", flush=True)
                        game["poll_active"] = True
                else:
                    print(f"[GAMES] Not yet time to poll {game['home']} vs {game['away']}", flush=True)
                    continue

                finished = find_finished_for_game(game, all_scores)
                if finished:
                    now_utc = datetime.datetime.now(UTC)
                    if now_utc - last_send_time < send_interval:
                        print("[DISCORD] Skipping send this tick to respect 20s interval", flush=True)
                        continue

                    print(f"[GAMES] Detected done: {finished['home']} vs {finished['away']} scores={finished['scores']}", flush=True)
                    send_discord_webhook(finished)
                    game["notified"] = True
                    last_send_time = now_utc
                    break  # one send per tick; rest will drain on future ticks
                else:
                    print(f"[DEBUG] {game['home']} vs {game['away']} has no scores yet", flush=True)

        print(f"[HEARTBEAT] {now_central_str()} Loop iteration complete", flush=True)
        time.sleep(60)


if __name__ == "__main__":
    main()
