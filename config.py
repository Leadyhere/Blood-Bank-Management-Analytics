"""Small, environment-based configuration; all database dates use UTC."""
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name('.env'))


def positive_setting(name, default, maximum=3650):
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        raise ValueError(f'{name} must be an integer from 1 to {maximum}.') from None
    if not 1 <= value <= maximum:
        raise ValueError(f'{name} must be an integer from 1 to {maximum}.')
    return value


MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
MONGODB_DB = os.getenv('MONGODB_DB', 'blood_bank_analytics')
SHELF_LIFE_DAYS = positive_setting('SHELF_LIFE_DAYS', 42)
LOW_STOCK_THRESHOLD = positive_setting('LOW_STOCK_THRESHOLD', 5, maximum=1000000)
NEAR_EXPIRY_DAYS = positive_setting('NEAR_EXPIRY_DAYS', 7)
BLOOD_GROUPS = ('A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-')
GENDERS = ('Female', 'Male', 'Other', 'Prefer not to say')


def utcnow():
    return datetime.now(timezone.utc)
