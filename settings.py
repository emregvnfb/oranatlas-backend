import os
from dotenv import load_dotenv

load_dotenv()

APP_NAME = "OranAtlas Backend"
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Europe/Istanbul")
FLASK_DEBUG = os.getenv("FLASK_DEBUG", "0") == "1"
SECRET_KEY = os.getenv("SECRET_KEY", "oranatlas-dev-secret")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "oranatlas")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")
API_FOOTBALL_BASE_URL = os.getenv("API_FOOTBALL_BASE_URL", "https://v3.football.api-sports.io")

THE_ODDS_API_KEY = os.getenv("THE_ODDS_API_KEY", "")
THE_ODDS_BASE_URL = os.getenv("THE_ODDS_BASE_URL", "https://api.the-odds-api.com/v4")

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "597212")
DEFAULT_MATCH_LOOKAHEAD_DAYS = int(os.getenv("DEFAULT_MATCH_LOOKAHEAD_DAYS", "2"))
