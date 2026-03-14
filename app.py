import os
import time
import datetime
from typing import Dict, List, Optional

import requests
from dateutil import parser

ODDS_API_KEY = os.getenv("ODDS_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

SPORT_KEY = "basketball_ncaab"
SCORES_ENDPOINT = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/scores"


def iso_to_utc_dt(iso_str: str) -> datetime.datetime:
    return parser.isoparse(iso_str).replace(tzinfo=None)


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
    today = datetime.datetime.utcnow().date()

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
    now = datetime.datetime.utcnow()
    return now >= poll_start


def poll_game(game: Dict) -> Optional[Dict]:
    """Check a single game; return finished game info or None."""
    params = {
        "apiKey": ODDS_API_KEY,
        "daysFrom": 1,
    }
    resp = requests.get(SCORES_ENDPOINT, params=params)
    resp.raise_for_status()
    data = resp.json()

    for g in data:
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

    games: List[Dict] = []
    last_refresh_date: Optional[datetime.date] = None

    while True:
        now = datetime.datetime.utcnow().date()

        # Refresh today's games once per day
        if last_refresh_date != now:
            games = fetch_todays_games()
            last_refresh_date = now

        # Every 2 minutes: poll games that should be active
        current_minute = datetime.datetime.utcnow().minute
        if current_minute % 2 == 0:
            for game in games:
                if game["notified"]:
                    continue

                if should_start_polling(game["start_time"]):
                    game["poll_active"] = True

                if not game["poll_active"]:
                    continue

                finished = poll_game(game)
                if finished:
                    send_discord_webhook(finished)
                    game["notified"] = True

        time.sleep(60)


if __name__ == "__main__":
    main()
