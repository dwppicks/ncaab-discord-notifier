import os
import time
import datetime
from typing import Dict, List, Optional

import requests
from dateutil import parser
from zoneinfo import ZoneInfo  # Python 3.9+

# Timezones
UTC = datetime.UTC
CENTRAL = ZoneInfo("America/Chicago")

# Env vars
ODDS_API_KEY = os.getenv("ODDS_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# Odds API config
SPORT_KEY = "basketball_ncaab"
SCORES_ENDPOINT = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/scores"


def iso_to_utc_dt(iso_str: str) -> datetime.datetime:
    """Parse ISO8601 string from The Odds API into timezone-aware UTC datetime."""
    return parser.isoparse(iso_str).astimezone(UTC)


def format_game_time_central(start_iso: str) -> str:
    """Return a human-readable CST/CDT time string for a game's commence_time."""
    utc_dt = iso_to_utc_dt(start_iso)
    central_dt = utc_dt.astimezone(CENTRAL)
    return central_dt.strftime("%Y-%m-%d %I:%M %p %Z")


def now_central_str() -> str:
    """Current time in US Central as a readable string."""
    now_central = datetime.datetime.now(UTC).astimezone(CENTRAL)
    return now_central.strftime("%Y-%m-%d %I:%M:%S %p %Z")


def fetch_todays_games_central() -> List[Dict]:
    """
    Fetch games and keep only those whose tip date is 'today' in Central time,
    regardless of UTC date. Called once on startup.
    """
    params = {
        "apiKey": ODDS_API_KEY,
        "daysFrom": 1,
    }
    resp = requests.get(SCORES_ENDPOINT, params=params)
    resp.raise_for_status()
    data = resp.json()

    games: List[Dict] = []

    today_central = datetime.datetime.now(CENTRAL).date()

    for g in data:
        commence = g.get("commence_time")
        if not commence:
            continue

        utc_start = iso_to_utc_dt(commence)
        central_start = utc_start.astimezone(CENTRAL)
        if central_start.date() != today_central:
            continue

        games.append(
            {
                "id": g["id"],
                "home": g["home_team"],
                "away": g["away_team"],
                "start_time": commence,  # ISO string (UTC)
                "poll_active": False,
                "notified": False,
            }
        )

    print(
        f"[INIT] {now_central_str()} Fetched {len(games)} games for today "
        f"(Central date {today_central}).",
        flush=True,
    )
    for game in games:
        print(
            f"[INIT] Tracking {game['home']} vs {game['away']} "
            f"at {format_game_time_central(game['start_time'])}",
            flush=True,
        )

    return games


def should_start_polling(start_iso: str, delay_hours: float = 1.5) -> bool:
    """Return True if we should begin polling this game (start + delay)."""
    start_dt = iso_to_utc_dt(start_iso)
    delay = datetime.timedelta(hours=delay_hours)
    poll_start = start_dt + delay
    now = datetime.datetime.now(UTC)
    return now >= poll_start


def poll_all_games() -> List[Dict]:
    """Call /scores once and return the full list, logging remaining credits."""
    params = {
        "apiKey": ODDS_API_KEY,
        "daysFrom": 1,
    }
    resp = requests.get(SCORES_ENDPOINT, params=params)
    resp.raise_for_status()

    remaining = resp.headers.get("x-requests-remaining")
    used = resp.headers.get("x-requests-used")
    last_cost = resp.headers.get("x-requests-last")
    print(
        f"[ODDS] Remaining={remaining}, Used={used}, LastCost={last_cost}",
        flush=True,
    )

    return resp.json()


def find_finished_for_game(game: Dict, all_scores: List[Dict]) -> Optional[Dict]:
    """
    From the full /scores response, treat any game with a scores array as done.
    The Odds API's NCAAB feed often leaves status=None even for completed games.
    """
    for g in all_scores:
        if g.get("id") != game["id"]:
            continue

        scores = g.get("scores")
        if scores:
            return {
                "id": g["id"],
                "home": g["home_team"],
                "away": g["away_team"],
                "status": g.get("status"),  # often None for NCAAB
                "scores": scores,
                "start_time": g.get("commence_time"),
            }

    return None


def send_discord_webhook(game: Dict):
    scores = game.get("scores", [])
    home_score = next(
        (s["score"] for s in scores if s["name"] == game["home"]), "?"
    )
    away_score = next(
        (s["score"] for s in scores if s["name"] == game["away"]), "?"
    )

    tip_central = (
        format_game_time_central(game["start_time"])
        if game.get("start_time")
        else "unknown time"
    )

    content = (
        f"✅ NCAAB Final: {game['home']} {home_score} "
        f"- {game['away']} {away_score} "
        f"(tip: {tip_central})"
    )

    resp = requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
    resp.raise_for_status()
    print(
        f"[DISCORD] Sent final notification for "
        f"{game['home']} vs {game['away']}",
        flush=True,
    )


def main():
    if not ODDS_API_KEY or not DISCORD_WEBHOOK_URL:
        raise RuntimeError("Missing ODDS_API_KEY or DISCORD_WEBHOOK_URL")

    # One-time startup notification
    try:
        startup_msg = {
            "content": "🚀 NCAAB notifier started on Render and is now polling."
        }
        r = requests.post(DISCORD_WEBHOOK_URL, json=startup_msg)
        r.raise_for_status()
        print("[DISCORD] Sent startup notification", flush=True)
    except Exception as e:
        print(f"[DISCORD] Failed to send startup notification: {e}", flush=True)

    # Fetch today's games in Central time once per restart
    print(
        f"[INIT] {now_central_str()} Loading today's games (Central)...",
        flush=True,
    )
    try:
        games: List[Dict] = fetch_todays_games_central()
    except Exception as e:
        print(f"[INIT] Error fetching today's games: {e}", flush=True)
        games = []

    while True:
        # Every 2 minutes: poll games that should be active
        current_minute = datetime.datetime.now(UTC).minute
        if current_minute % 2 == 0 and games:
            print("=== Polling loop tick ===", flush=True)
            active_games = [g for g in games if not g["notified"]]
            print(
                f"[GAMES] Tracking {len(active_games)} games not yet notified",
                flush=True,
            )

            # Single /scores call for all games
            try:
                all_scores = poll_all_games()
            except Exception as e:
                print(f"[ODDS] Error calling scores endpoint: {e}", flush=True)
                time.sleep(60)
                continue

            for game in active_games:
                if should_start_polling(game["start_time"]):
                    if not game["poll_active"]:
                        print(
                            f"[GAMES] {now_central_str()} Activating polling for "
                            f"{game['home']} vs {game['away']} "
                            f"({format_game_time_central(game['start_time'])})",
                            flush=True,
                        )
                    game["poll_active"] = True

                if not game["poll_active"]:
                    print(
                        f"[GAMES] Not yet time to poll "
                        f"{game['home']} vs {game['away']} "
                        f"({format_game_time_central(game['start_time'])})",
                        flush=True,
                    )
                    continue

                finished = find_finished_for_game(game, all_scores)
                if finished:
                    print(
                        f"[GAMES] Detected done (status={finished['status']}): "
                        f"{finished['home']} vs {finished['away']} "
                        f"with scores={finished['scores']}",
                        flush=True,
                    )
                    send_discord_webhook(finished)
                    game["notified"] = True
                else:
                    print(
                        f"[DEBUG] Game {game['home']} vs {game['away']} "
                        f"has no scores yet",
                        flush=True,
                    )

        print(
            f"[HEARTBEAT] {now_central_str()} Loop iteration complete",
            flush=True,
        )
        time.sleep(60)


if __name__ == "__main__":
    main()
