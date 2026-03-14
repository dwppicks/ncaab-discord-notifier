import os
import time
import datetime
from typing import Dict, List, Optional

import requests
from dateutil import parser
import datetime

UTC = datetime.UTC  # Python 3.11+ built-in UTC tzinfo


ODDS_API_KEY = os.getenv("ODDS_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

SPORT_KEY = "basketball_ncaab"
SCORES_ENDPOINT = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/scores"


def iso_to_utc_dt(iso_str: str) -> datetime.datetime:
    return parser.isoparse(iso_str).astimezone(UTC)


def fetch_todays_games() -> List[Dict]:
    """Fetch today's games once, using the scores endpoint."""
    params = {
        "apiKey": ODDS_API_KEY,
        "daysFrom": 1,  # today and near future
    }
    resp = requests.get(SCORES_ENDPOINT, params=params)
    resp.raise_for_status()
    data = resp.json()

    games = []
    today = datetime.datetime.now(UTC).date()

    for g in data:
        if not g.get("commence_time"):
            continue
        start_dt = iso_to_utc_dt(g["commence_time"])
        if start_dt.date() != today:
            continue

        games.append(
            {
                "id": g["id"],
                "home": g["home_team"],
                "away": g["away_team"],
                "start_time": g["commence_time"],
                "poll_active": False,
                "notified": False,
            }
        )

    print(f"Fetched {len(games)} games for today.")
    return games


def should_start_polling(start_iso: str, delay_hours: float = 1.5) -> bool:
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
    print(f"[ODDS] Remaining={remaining}, Used={used}, LastCost={last_cost}")

    return resp.json()


def find_finished_for_game(game: Dict, all_scores: List[Dict]) -> Optional[Dict]:
    for g in all_scores:
        if g.get("id") != game["id"]:
            continue

        status = g.get("status")
        if status == "STATUS_FINAL":
            return {
                "id": g["id"],
                "home": g["home_team"],
                "away": g["away_team"],
                "status": status,
                "scores": g.get("scores", []),
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

    content = (
        f"✅ NCAAB Final: {game['home']} {home_score} "
        f"- {game['away']} {away_score}"
    )

    resp = requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
    resp.raise_for_status()
    print(f"Sent Discord notification for {game['home']} vs {game['away']}")


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
        print("[DISCORD] Sent startup notification")
    except Exception as e:
        print(f"[DISCORD] Failed to send startup notification: {e}")

    games: List[Dict] = []
    last_refresh_date: Optional[datetime.date] = None

    while True:
        now = datetime.datetime.now(UTC).date()

        # Refresh today's games once per day
        if last_refresh_date != now:
            games = fetch_todays_games()
            last_refresh_date = now

        # Every 2 minutes: poll games that should be active
        current_minute = datetime.datetime.now(UTC).minute
        if current_minute % 2 == 0 and games:
            print("=== Polling loop tick ===")
            active_games = [g for g in games if not g["notified"]]
            print(f"[GAMES] Tracking {len(active_games)} games not yet notified")

            # Single /scores call for all games
            try:
                all_scores = poll_all_games()
            except Exception as e:
                print(f"[ODDS] Error calling scores endpoint: {e}")
                time.sleep(60)
                continue

            for game in active_games:
                if should_start_polling(game["start_time"]):
                    if not game["poll_active"]:
                        print(f"[GAMES] Activating polling for {game['home']} vs {game['away']}")
                    game["poll_active"] = True

                if not game["poll_active"]:
                    print(f"[GAMES] Not yet time to poll {game['home']} vs {game['away']}")
                    continue

                finished = find_finished_for_game(game, all_scores)
                if finished:
                    print(f"[GAMES] Detected final: {finished['home']} vs {finished['away']}")
                    send_discord_webhook(finished)
                    game["notified"] = True


        time.sleep(60)


if __name__ == "__main__":
    main()
